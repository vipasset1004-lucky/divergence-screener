"""
실시간 다이버전스 스크리너 웹 서버
- Flask + SSE로 브라우저에서 실시간 스캔
- /api/scan → 스캔 실행 + 실시간 결과 스트리밍
"""

from flask import Flask, Response, request, send_file, jsonify
import pandas as pd
import yfinance as yf
from weekly_divergence_screener import (
    get_krx_tickers, get_fallback_tickers, get_new_listings,
    fetch_weekly_data, fetch_daily_data,
    calculate_indicators, detect_bullish_divergence, score_100, score_daily,
    US_THEMES
)
import json
import time
import threading
import gc
import os
import math
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

# ── Redis 캐시 (Upstash) ─────────────────────────────────────────────────────
# 환경변수 UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN 이 있으면 Redis 사용,
# 없으면 로컬 파일 폴백 (개발 환경용)
_redis = None
try:
    _url = os.environ.get("UPSTASH_REDIS_REST_URL")
    _tok = os.environ.get("UPSTASH_REDIS_REST_TOKEN")
    if _url and _tok:
        from upstash_redis import Redis
        _redis = Redis(url=_url, token=_tok)
        print("[cache] Upstash Redis 연결됨")
    else:
        print("[cache] Redis 환경변수 없음 → 파일 폴백")
except Exception as _e:
    print(f"[cache] Redis 초기화 실패 → 파일 폴백: {_e}")

CACHE_KEY      = "screener:cache"
CACHE_PREV_KEY = "screener:cache_prev"
CACHE_TTL      = 60 * 60 * 50   # 50시간 (다음 21시 스캔 전까지 충분히)
CACHE_PATH      = "scan_results_cache.json"       # 파일 폴백용
CACHE_PREV_PATH = "scan_results_cache_prev.json"  # 파일 폴백용


def _cache_get(key):
    """Redis 또는 파일에서 캐시 읽기"""
    if _redis:
        try:
            val = _redis.get(key)
            return json.loads(val) if val else None
        except Exception as e:
            print(f"[cache] get 오류: {e}")
    # 파일 폴백
    path = CACHE_PATH if key == CACHE_KEY else CACHE_PREV_PATH
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _cache_set(key, value):
    """Redis 또는 파일에 캐시 저장"""
    if _redis:
        try:
            _redis.set(key, json.dumps(value, ensure_ascii=False), ex=CACHE_TTL)
            return
        except Exception as e:
            print(f"[cache] set 오류: {e}")
    # 파일 폴백
    path = CACHE_PATH if key == CACHE_KEY else CACHE_PREV_PATH
    with open(path, "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False)
# ─────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)

# 스캔 상태 관리
scan_state = {
    "running": False,
    "last_results": None,
    "last_scan_date": None,
}


@app.route("/")
def index():
    return send_file("divergence_dashboard.html")


@app.route("/api/detail")
def stock_detail():
    """종목 상세 분석 - 스코어링 + 실적 (차트 없음, 메모리 절약)"""
    ticker = request.args.get("ticker", "")
    is_korean = request.args.get("kr", "true") == "true"

    if not ticker:
        return jsonify({"error": "ticker required"})

    try:
        df_w = fetch_weekly_data(ticker, is_korean=is_korean)
        if df_w is None:
            return jsonify({"error": "no data"})

        if isinstance(df_w.columns, pd.MultiIndex):
            df_w.columns = df_w.columns.get_level_values(0)

        df_calc = calculate_indicators(df_w)
        s = score_100(df_calc) if df_calc is not None else None
        div = detect_bullish_divergence(df_calc) if df_calc is not None else None
        del df_w, df_calc

        df_d = fetch_daily_data(ticker, is_korean=is_korean)
        d = score_daily(df_d) if df_d is not None else None
        del df_d

        earnings = get_earnings(ticker, is_korean=is_korean)
        gc.collect()

        return jsonify({
            "ticker": ticker,
            "weekly_score": s,
            "daily_score": d,
            "divergence": {"count": div["divergence_count"], "indicators": list(div["divergences"].keys())} if div else None,
            "earnings": earnings,
        })
    except Exception as e:
        return jsonify({"error": str(e)})


def get_earnings(ticker, is_korean=True):
    """실적 데이터 수집 - 매출/영업이익 추세"""
    try:
        symbol = f"{ticker}.KS" if is_korean else ticker
        t = yf.Ticker(symbol)

        info = t.info or {}
        fin = t.financials

        result = {
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            "profit_margin": info.get("profitMargins"),
            "operating_margin": info.get("operatingMargins"),
            "forward_pe": info.get("forwardPE"),
            "market_cap": info.get("marketCap"),
        }

        # 연간 매출/영업이익 추세 (최근 4년)
        if fin is not None and not fin.empty:
            revenue = []
            op_income = []
            years = []
            for col in fin.columns[:4]:
                yr = col.year if hasattr(col, 'year') else str(col)[:4]
                years.append(str(yr))

                rev = fin.loc["Total Revenue", col] if "Total Revenue" in fin.index else None
                oi = fin.loc["Operating Income", col] if "Operating Income" in fin.index else None

                revenue.append(round(float(rev / 1e8), 0) if rev == rev and rev is not None else None)
                op_income.append(round(float(oi / 1e8), 0) if oi == oi and oi is not None else None)

            result["years"] = years
            result["revenue"] = revenue  # 억원
            result["op_income"] = op_income  # 억원

            # 실적 구분 판단
            if len(op_income) >= 2 and op_income[0] is not None and op_income[1] is not None:
                if op_income[0] > 0 and op_income[1] > 0:
                    if op_income[0] > op_income[1] * 1.2:
                        result["earnings_type"] = "실적 급증"
                    elif op_income[0] > op_income[1]:
                        result["earnings_type"] = "실적 증가"
                    else:
                        result["earnings_type"] = "실적 감소"
                elif op_income[0] > 0 and op_income[1] <= 0:
                    result["earnings_type"] = "흑자 전환"
                elif op_income[0] <= 0:
                    result["earnings_type"] = "적자"
                else:
                    result["earnings_type"] = "-"
            else:
                result["earnings_type"] = "-"
        else:
            result["earnings_type"] = "-"
            result["years"] = []
            result["revenue"] = []
            result["op_income"] = []

        return result
    except:
        return {"earnings_type": "-", "years": [], "revenue": [], "op_income": [],
                "revenue_growth": None, "earnings_growth": None}


@app.route("/api/results")
def get_results():
    if scan_state["last_results"]:
        return jsonify({
            "scan_date": scan_state["last_scan_date"],
            "total_found": len(scan_state["last_results"]),
            "results": scan_state["last_results"]
        })
    return jsonify({"error": "no data", "results": []})


@app.route("/api/scan_single")
def scan_single():
    """단일 종목 스캔 - 검색창 + 스캔 버튼용"""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "query required"})

    # 티커 숫자인지 판단
    is_ticker = q.isdigit() and len(q) == 6

    # 전체 리스트에서 매칭 (이름 or 티커)
    all_tickers = get_fallback_tickers(1000)
    match = None
    if is_ticker:
        for t in all_tickers:
            if t["ticker"] == q:
                match = t
                break
        if not match:
            match = {"ticker": q, "name": q, "themes": []}
    else:
        qlow = q.lower()
        for t in all_tickers:
            if qlow in t["name"].lower():
                match = t
                break
        if not match:
            # 미국 주식 대문자 티커 시도
            qup = q.upper()
            if qup in US_THEMES:
                match = {"ticker": qup, "name": qup, "themes": US_THEMES[qup]}

    if not match:
        return jsonify({"error": f"'{q}' 매칭 실패 (이름/티커 확인)"})

    ticker = match["ticker"]
    name = match["name"]
    is_korean = ticker.isdigit() and len(ticker) == 6

    try:
        # 주봉 분석
        df_w = fetch_weekly_data(ticker, is_korean=is_korean)
        if df_w is None:
            return jsonify({"error": "no weekly data"})
        df_w = calculate_indicators(df_w)
        if df_w is None:
            return jsonify({"error": "indicator calc failed"})
        s100 = score_100(df_w)

        # 주봉 3M 수익률
        close_vals = df_w["Close"].values
        weekly_return_3m = None
        if len(close_vals) >= 13 and close_vals[-13] > 0:
            weekly_return_3m = round(((close_vals[-1] / close_vals[-13]) - 1) * 100, 1)

        # 다이버전스
        div = detect_bullish_divergence(df_w)
        div_data = {}
        if div:
            div_data = {
                "divergence_count": div["divergence_count"],
                "divergences": {k: v for k, v in div["divergences"].items()},
                "div_score": div["score"],
                "bonus_signals": div.get("bonus_signals", []),
            }
        del df_w

        # 일봉 (fetch 실패 시 daily_score=None)
        df_d = fetch_daily_data(ticker, is_korean=is_korean)
        daily = score_daily(df_d) if df_d is not None else {
            "daily_score": None, "daily_signals": [], "daily_rsi": 0, "daily_vol_ratio": 0,
            "return_3m": None, "vol_trend_60d": 1.0, "is_overheated": False
        }
        del df_d

        # 실적
        earnings = get_earnings(ticker, is_korean=is_korean)
        gc.collect()

        import math
        themes = match.get("themes", [])
        result = {"ticker": ticker, "name": name, "themes": themes, **s100, **daily, **div_data,
                  "earnings_type": earnings.get("earnings_type", "-"),
                  "revenue_growth": earnings.get("revenue_growth"),
                  "earnings_growth": earnings.get("earnings_growth"),
                  "operating_margin": earnings.get("operating_margin")}

        if result.get("return_3m") is None or (isinstance(result.get("return_3m"), float) and math.isnan(result["return_3m"])):
            if weekly_return_3m is not None:
                result["return_3m"] = weekly_return_3m
                result["is_overheated"] = bool(weekly_return_3m > 50)
            else:
                result["return_3m"] = 0

        d_score = daily["daily_score"] if daily["daily_score"] is not None else 0
        result["total_score"] = round(s100["score_10"] * 0.7 + d_score * 0.3, 1)
        return jsonify({"result": result})
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/cached-results")
def cached_results():
    """자동 스캔으로 저장된 결과 반환"""
    try:
        data = _cache_get(CACHE_KEY)
        if not data or not data.get("results"):
            return jsonify({"results": [], "scan_date": None, "total": 0})
        prev_tickers = set()
        prev = _cache_get(CACHE_PREV_KEY)
        if prev:
            prev_tickers = {r["ticker"] for r in prev.get("results", [])}
        data["prev_tickers"] = list(prev_tickers)
        return jsonify(data)
    except Exception as e:
        return jsonify({"results": [], "scan_date": None, "total": 0, "error": str(e)})


def auto_scan_job():
    """매일 21:00 자동 스캔 (527개 전체 + 신규상장) → Redis/파일 저장"""
    # 이미 오늘 스캔됐으면 건너뜀 (worker 중복 방지)
    try:
        cached = _cache_get(CACHE_KEY)
        if cached:
            saved_date = cached.get("scan_date", "")[:10]
            if saved_date == datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d"):
                print("[auto_scan] 오늘 이미 스캔됨, 건너뜀")
                return
    except Exception:
        pass

    if scan_state["running"]:
        print("[auto_scan] 수동 스캔 중, 건너뜀")
        return

    scan_state["running"] = True
    print(f"[auto_scan] 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    results = []

    try:
        tickers = get_fallback_tickers(9999)  # 전체 (527개)
        existing = {t["ticker"] for t in tickers}
        try:
            if os.path.exists("new_listings_cache.json"):
                with open("new_listings_cache.json", "r", encoding="utf-8") as f:
                    nl_cached = json.load(f)
                new_listings = nl_cached.get("listings", [])
                new_listings = [l for l in new_listings if l["ticker"] not in existing]
                tickers = tickers + new_listings
        except Exception:
            pass

        min_score = 2.5
        candidates = []

        # 1단계: 주봉 스캔
        for i, t in enumerate(tickers):
            ticker = t["ticker"]
            name = t["name"]
            if i % 50 == 0:
                print(f"[auto_scan] 1단계 {i+1}/{len(tickers)} ({name})")
            try:
                df = fetch_weekly_data(ticker, is_korean=True)
                if df is None:
                    continue
                df = calculate_indicators(df)
                if df is None:
                    continue
                s100 = score_100(df)
                effective_min = 1.5 if t.get("is_new_listing") else min_score
                if not s100 or s100["score_10"] < effective_min:
                    del df
                    continue
                close_vals = df["Close"].values
                weekly_return_3m = None
                if len(close_vals) >= 13 and close_vals[-13] > 0:
                    weekly_return_3m = round(((close_vals[-1] / close_vals[-13]) - 1) * 100, 1)
                div_data = {}
                if s100["score_10"] >= 2.0:
                    div = detect_bullish_divergence(df)
                    if div:
                        div_data = {
                            "divergence_count": div["divergence_count"],
                            "divergences": {k: v for k, v in div["divergences"].items()},
                            "div_score": div["score"],
                            "bonus_signals": div.get("bonus_signals", []),
                        }
                candidates.append({"t": t, "s100": s100, "weekly_return_3m": weekly_return_3m, "div_data": div_data})
                del df
            except Exception:
                pass
            if i % 20 == 0:
                gc.collect()
            time.sleep(0.15)

        gc.collect()
        print(f"[auto_scan] 1단계 완료: {len(candidates)}개 후보")

        # 2단계: 정밀 분석
        for j, cand in enumerate(candidates):
            t = cand["t"]
            s100 = cand["s100"]
            weekly_return_3m = cand["weekly_return_3m"]
            ticker = t["ticker"]
            name = t["name"]
            if j % 20 == 0:
                print(f"[auto_scan] 2단계 {j+1}/{len(candidates)} ({name})")
            try:
                div_data = cand["div_data"]
                df_daily = fetch_daily_data(ticker, is_korean=True)
                daily = score_daily(df_daily) if df_daily is not None else {
                    "daily_score": None, "daily_signals": [], "daily_rsi": 0, "daily_vol_ratio": 0,
                    "return_3m": None, "vol_trend_60d": 1.0, "is_overheated": False
                }
                del df_daily
                earnings = get_earnings(ticker, is_korean=True) if s100["score_10"] >= 3.0 else {
                    "earnings_type": "-", "revenue_growth": None, "earnings_growth": None, "operating_margin": None
                }
                themes = t.get("themes", [])
                result = {"ticker": ticker, "name": name, "themes": themes, **s100, **daily, **div_data,
                          "earnings_type": earnings.get("earnings_type", "-"),
                          "revenue_growth": earnings.get("revenue_growth"),
                          "earnings_growth": earnings.get("earnings_growth"),
                          "operating_margin": earnings.get("operating_margin"),
                          "is_new_listing": bool(t.get("is_new_listing")),
                          "listing_date": t.get("listing_date"),
                          "weeks_available": t.get("weeks_available")}
                if result.get("return_3m") is None or (isinstance(result.get("return_3m"), float) and math.isnan(result["return_3m"])):
                    if weekly_return_3m is not None:
                        result["return_3m"] = weekly_return_3m
                        result["is_overheated"] = bool(weekly_return_3m > 50)
                    else:
                        result["return_3m"] = 0
                d_score = daily["daily_score"] if daily["daily_score"] is not None else 0
                result["total_score"] = round(s100["score_10"] * 0.7 + d_score * 0.3, 1)
                results.append(result)
            except Exception as e:
                print(f"[auto_scan] 2단계 오류 {name}: {e}")
            if j % 5 == 0:
                gc.collect()
            time.sleep(0.2)

        results.sort(key=lambda x: x.get("total_score", 0), reverse=True)
        scan_date = datetime.now().isoformat()
        scan_state["last_results"] = results
        scan_state["last_scan_date"] = scan_date

        # 오늘 캐시를 어제 키로 보관 후 새 결과 저장
        today_cache = _cache_get(CACHE_KEY)
        if today_cache:
            _cache_set(CACHE_PREV_KEY, today_cache)
        _cache_set(CACHE_KEY, {"results": results, "scan_date": scan_date, "total": len(results)})
        print(f"[auto_scan] 완료: {len(results)}개 → Redis 저장")

    except Exception as e:
        print(f"[auto_scan] 오류: {e}")
    finally:
        scan_state["running"] = False


# 자동 스캔 스케줄러 (매일 21:00 KST)
_scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Seoul"))
_scheduler.add_job(auto_scan_job, "cron", hour=21, minute=0, id="daily_scan")
_scheduler.start()


@app.route("/api/scan")
def scan():
    market = request.args.get("market", "KR")
    top_n = int(request.args.get("top_n", "100"))
    min_score = float(request.args.get("min_score", "2.0"))

    if scan_state["running"]:
        def already_running():
            yield f"data: {json.dumps({'type': 'error', 'message': '이미 스캔이 진행 중입니다'})}\n\n"
        return Response(already_running(), mimetype="text/event-stream")

    def generate():
        scan_state["running"] = True
        results = []
        try:
            # 1단계: 종목 리스트 수집
            yield f"data: {json.dumps({'type': 'status', 'message': '종목 리스트 수집 중...', 'progress': 0}, ensure_ascii=False)}\n\n"

            if market.upper() == "US":
                tickers = [
                    {"ticker": t, "name": t} for t in [
                        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
                        "AMD", "PLTR", "SOFI", "COIN", "MARA", "RIOT", "SHOP",
                        "NET", "SNOW", "DDOG", "CRWD", "ZS", "PANW", "ABNB",
                        "UBER", "RBLX", "U", "HOOD", "AFRM", "UPST", "IONQ",
                        "SMCI", "ARM", "AVGO", "MU", "MRVL", "QCOM", "INTC",
                    ]
                ]
                is_korean = False
            else:
                # fallback 리스트 + 신규상장(캐시 있으면 즉시, 없으면 백그라운드 재생성)
                tickers = get_fallback_tickers(top_n)
                existing = {t["ticker"] for t in tickers}
                try:
                    cache_path = "new_listings_cache.json"
                    new_listings = []
                    need_refresh = False

                    if os.path.exists(cache_path):
                        with open(cache_path, "r", encoding="utf-8") as _f:
                            _cached = _f.read()
                        _cached = json.loads(_cached)
                        listings = _cached.get("listings", [])
                        updated = datetime.fromisoformat(_cached.get("updated_at", "1970-01-01"))

                        # 구버전 감지: themes가 ["신규상장"] 단독인 항목
                        is_old_format = any(l.get("themes") == ["신규상장"] for l in listings[:5])
                        is_expired = (datetime.now() - updated).days >= 7

                        if is_old_format:
                            print("[scan] 신규상장 구버전 캐시 감지 → 백그라운드 갱신")
                            need_refresh = True
                            new_listings = []  # 구버전은 쓰지 않음
                        elif is_expired:
                            print("[scan] 신규상장 캐시 만료 → 백그라운드 갱신, 구 캐시 사용")
                            need_refresh = True
                            new_listings = listings  # 만료됐어도 데이터는 사용
                        else:
                            new_listings = listings
                    else:
                        print("[scan] 신규상장 캐시 없음 → 백그라운드 생성 (다음 스캔부터 포함)")
                        need_refresh = True

                    if need_refresh:
                        def _refresh_cache():
                            try:
                                get_new_listings(existing, force_refresh=True)
                            except Exception:
                                pass
                        threading.Thread(target=_refresh_cache, daemon=True).start()

                    if new_listings:
                        tickers = tickers + new_listings
                except Exception as e:
                    print(f"[scan] 신규상장 로드 실패(무시): {e}")
                is_korean = True

            total = len(tickers)
            yield f"data: {json.dumps({'type': 'status', 'message': f'{total}개 종목 분석 시작', 'progress': 5, 'total': total}, ensure_ascii=False)}\n\n"

            # ━━ STAGE 1: 주봉 빠른 스캔 (메모리 최소) ━━━━━━━━━━
            import math
            candidates = []  # 1단계 통과 종목
            yield f"data: {json.dumps({'type': 'status', 'message': '1단계: 주봉 스캔 중...', 'progress': 5}, ensure_ascii=False)}\n\n"

            for i, t in enumerate(tickers):
                ticker = t["ticker"]
                name = t["name"]
                progress = int(5 + (i / total) * 55)

                if i % 10 == 0:
                    yield f"data: {json.dumps({'type': 'progress', 'current': name, 'index': i+1, 'total': total, 'progress': progress, 'found': len(candidates)}, ensure_ascii=False)}\n\n"

                try:
                    df = fetch_weekly_data(ticker, is_korean=is_korean)
                    if df is None:
                        continue
                    df = calculate_indicators(df)
                    if df is None:
                        continue

                    s100 = score_100(df)
                    if not s100 or s100["score_100"] <= 0 or len(s100["signals"]) < 1:
                        del df
                        continue
                    # 신규상장은 데이터 부족으로 점수가 낮을 수 있어 별도 기준 적용
                    effective_min = 1.5 if t.get("is_new_listing") else min_score
                    if s100["score_10"] < effective_min:
                        del df
                        continue

                    # 주봉 return_3m 계산
                    close_vals = df["Close"].values
                    weekly_return_3m = None
                    if len(close_vals) >= 13 and close_vals[-13] > 0:
                        weekly_return_3m = round(((close_vals[-1] / close_vals[-13]) - 1) * 100, 1)

                    # 다이버전스도 1단계에서 처리 (주봉 데이터 재로드 방지)
                    div_data = {}
                    if s100["score_10"] >= 2.0:
                        div = detect_bullish_divergence(df)
                        if div:
                            div_data = {
                                "divergence_count": div["divergence_count"],
                                "divergences": {k: v for k, v in div["divergences"].items()},
                                "div_score": div["score"],
                                "bonus_signals": div.get("bonus_signals", []),
                            }

                    candidates.append({"t": t, "s100": s100, "weekly_return_3m": weekly_return_3m, "div_data": div_data})
                    del df
                except:
                    pass

                if i % 20 == 0:
                    gc.collect()
                time.sleep(0.15)

            gc.collect()
            yield f"data: {json.dumps({'type': 'status', 'message': f'1단계 완료: {len(candidates)}개 후보', 'progress': 60}, ensure_ascii=False)}\n\n"

            # ━━ STAGE 2: 후보만 정밀 분석 ━━━━━━━━━━━━━━━
            yield f"data: {json.dumps({'type': 'status', 'message': f'2단계: {len(candidates)}개 정밀 분석...', 'progress': 62}, ensure_ascii=False)}\n\n"

            for j, cand in enumerate(candidates):
                t = cand["t"]
                s100 = cand["s100"]
                weekly_return_3m = cand["weekly_return_3m"]
                ticker = t["ticker"]
                name = t["name"]
                progress = int(62 + (j / max(len(candidates), 1)) * 33)

                if j % 3 == 0:
                    yield f"data: {json.dumps({'type': 'progress', 'current': f'[정밀] {name}', 'index': j+1, 'total': len(candidates), 'progress': progress, 'found': len(results)}, ensure_ascii=False)}\n\n"

                try:
                    div_data = cand["div_data"]

                    # 일봉 타이밍 (fetch 실패 시 daily_score=None → 대시보드 GAP/콤보 필터에서 제외)
                    df_daily = fetch_daily_data(ticker, is_korean=is_korean)
                    daily = score_daily(df_daily) if df_daily is not None else {
                        "daily_score": None, "daily_signals": [], "daily_rsi": 0, "daily_vol_ratio": 0,
                        "return_3m": None, "vol_trend_60d": 1.0, "is_overheated": False
                    }
                    del df_daily

                    # 실적 (3점 이상만)
                    earnings = get_earnings(ticker, is_korean=is_korean) if s100["score_10"] >= 3.0 else {
                        "earnings_type": "-", "revenue_growth": None, "earnings_growth": None, "operating_margin": None
                    }

                    themes = t.get("themes", US_THEMES.get(ticker, []))
                    result = {"ticker": ticker, "name": name, "themes": themes, **s100, **daily, **div_data,
                              "earnings_type": earnings.get("earnings_type", "-"),
                              "revenue_growth": earnings.get("revenue_growth"),
                              "earnings_growth": earnings.get("earnings_growth"),
                              "operating_margin": earnings.get("operating_margin"),
                              "is_new_listing": bool(t.get("is_new_listing")),
                              "listing_date": t.get("listing_date"),
                              "weeks_available": t.get("weeks_available")}

                    # return_3m NaN 보정
                    if result.get("return_3m") is None or (isinstance(result.get("return_3m"), float) and math.isnan(result["return_3m"])):
                        if weekly_return_3m is not None:
                            result["return_3m"] = weekly_return_3m
                            result["is_overheated"] = bool(weekly_return_3m > 50)
                        else:
                            result["return_3m"] = 0

                    d_score = daily["daily_score"] if daily["daily_score"] is not None else 0
                    result["total_score"] = round(s100["score_10"] * 0.7 + d_score * 0.3, 1)
                    results.append(result)

                    yield f"data: {json.dumps({'type': 'found', 'result': result, 'found_total': len(results)}, ensure_ascii=False)}\n\n"

                except Exception as e:
                    print(f"Stage2 error {name}: {e}")

                if j % 5 == 0:
                    gc.collect()
                time.sleep(0.2)

            # 3단계: 완료
            results.sort(key=lambda x: x.get("total_score", 0), reverse=True)
            scan_state["last_results"] = results
            scan_state["last_scan_date"] = datetime.now().isoformat()

            yield f"data: {json.dumps({'type': 'complete', 'total_found': len(results), 'results': results, 'scan_date': scan_state['last_scan_date']}, ensure_ascii=False)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
        finally:
            scan_state["running"] = False

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8765))
    print("\n" + "=" * 50)
    print("  주봉 다이버전스 스크리너 서버")
    print(f"  http://localhost:{port}")
    print("=" * 50 + "\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
