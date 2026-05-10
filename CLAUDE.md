# CLAUDE.md — Divergence Screener (주봉 다이버전스)

## 프로젝트 개요

주봉 RSI 다이버전스를 핵심으로 텐버거·반전 후보를 발굴하는 스크리너.
일봉 다이버전스도 함께 보지만 주봉이 메인 신호.

- **운영**: GitHub Actions (cron + workflow_dispatch) + GitHub Pages
- **이전 운영**: Render (2026-05-10 GitHub Actions로 마이그레이션, $25/월 → $0)
- **라이브 URL**: https://vipasset1004-lucky.github.io/divergence-screener/
- **자동 스캔**: 평일 KST 21:00 (UTC 12:00)

## 기술 스택

- **스캔 엔진**: `weekly_divergence_screener.py` 단독 실행
- **데이터**: yfinance (한국 + 미국 주식)
- **호스팅**: GitHub Pages (gh-pages 브랜치)
- **비용**: $0 (public 레포)

## 자원 예산 (필수 준수)

> **"정해진 룸 안에서 최적의 안을 만드는 게 진짜 엔지니어링"**

- **타겟**: GitHub Actions ubuntu-latest (7GB RAM)
- **스캔 시간 목표**: 25분 이내 (timeout 30분)
- **유니버스 상한**: 1500종목 (KR ALL + 미국 추가 시 별도)

### 변경 시 체크리스트
1. yfinance 호출 부담 측정 (rate-limit 주의)
2. 데이터 의존성 추가 시 GitHub Actions 환경 호환성 확인
3. 출력 경로는 `os.environ.get("OUTPUT_DIR", os.getcwd())` 사용 (Linux 호환)

### 초과 시 대응 순서
1. universe 슬림화
2. yfinance 호출 줄이기 (필수 종목만)
3. 동시성 ↓
4. 마지막 수단: 인스턴스 타입 변경 (지금은 무료 ubuntu-latest)

## 작업 원칙

1. **이론 → 백테스트 → 알고리즘 → 코드** 순서
2. **백테스트 통과 못하면 알고리즘 회귀**
3. **자원 예산 우선** — 위 섹션 따를 것
4. 새 신호 추가 전 weekly + daily 양쪽 검증

## 폴더 구조

```
divergence-screener/
├── weekly_divergence_screener.py   # 스캔 엔진 (standalone)
├── divergence_dashboard.html       # 구버전 대시보드 (Flask용, 보존)
├── .github/workflows/
│   ├── scan.yml                    # cron 평일 21:00 + workflow_dispatch
│   └── deploy-frontend.yml         # 향후 frontend 변경 시 빠른 배포
└── CLAUDE.md                        # 이 파일
```

gh-pages 브랜치에 `index.html` (정적 frontend) + `divergence_results.json` (스캔 결과).

## 배포 흐름

1. Workflow 트리거 (cron 또는 수동)
2. `python weekly_divergence_screener.py KR 1500` 실행
3. `divergence_results.json` 생성
4. gh-pages 브랜치에 배포 (index.html 보존, JSON 교체)
5. 라이브 URL 자동 갱신
