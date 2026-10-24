"""빗썸 KRW 전 종목 EMA(9/20) 일봉 교차 → 텔레그램 알림.

실행: python bithumb_ema_bot.py
환경설정: config.env (config.env.example 복사)

시세 조회에는 빗썸 계정/API 키가 필요하지 않습니다.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / "config.env")
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
if not TOKEN or not CHAT_ID:
    raise SystemExit("config.env에 TELEGRAM_BOT_TOKEN과 TELEGRAM_CHAT_ID를 입력하세요.")

BITHUMB = "https://api.bithumb.com"
KST = timezone(timedelta(hours=9))
# 클라우드 볼륨이 장착되면 알림 기록을 영구 저장합니다.
# Railway: 서비스에 Volume을 만들고 /data 경로로 마운트하세요.
STATE_FILE = Path(
    os.getenv("ALERT_STATE_FILE")
    or (
        str(Path(os.environ["RAILWAY_VOLUME_MOUNT_PATH"]) / "alert_state.json")
        if os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
        else str(ROOT / "alert_state.json")
    )
)
POLL_SECONDS = 60                  # 1분 간격 스냅샷 조회 (정확한 초단위 실시간 아님)
HISTORY_REFRESH_SECONDS = 3600      # 종목과 일봉 이력은 매 1시간 새로고침
HISTORY_COUNT = 160               # 일봉 160개로 EMA 시드 편차 완화
MIN_COMPLETED_CANDLES = 60
HTTP = requests.Session()
HTTP.headers.update({"Accept": "application/json", "User-Agent": "bithumb-ema-alert/1.0"})
log = logging.getLogger("bithumb-ema")


def api_get(path: str, params: dict | None = None):
    for attempt in range(3):
        try:
            response = HTTP.get(f"{BITHUMB}{path}", params=params, timeout=12)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == 2:
                raise RuntimeError(f"빗썸 API 오류 {path}: {exc}") from exc
            time.sleep(1 + 2 * attempt)


def notify(message: str) -> bool:
    try:
        response = HTTP.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": message, "disable_web_page_preview": True},
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(str(payload))
        return True
    except (requests.RequestException, ValueError, RuntimeError) as exc:
        log.error("텔레그램 전송 실패: %s", exc)
        return False


def load_state() -> dict[str, set[str]]:
    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return {name: set(state.get(name, [])) for name in ("live", "confirmed")}
    except (FileNotFoundError, ValueError, OSError):
        return {"live": set(), "confirmed": set()}


def save_state(state: dict[str, set[str]]) -> None:
    # 한 달 정도의 중복 방지 기록만 유지합니다.
    cutoff = (datetime.now(KST) - timedelta(days=40)).date().isoformat()
    slimmed = {
        name: sorted(x for x in values if x.rsplit("|", 1)[-1][:10] >= cutoff)
        for name, values in state.items()
    }
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps(slimmed, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(STATE_FILE)


def send_once(state: dict[str, set[str]], kind: str, market: str, day: str, message: str):
    key = f"{market}|{day}"
    if key in state[kind]:
        return
    if notify(message):
        state[kind].add(key)
        save_state(state)
        log.info("알림 전송: %s / %s / %s", kind, market, day)


def calculate_ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    output = [float(values[0])]
    for value in values[1:]:
        output.append(alpha * float(value) + (1.0 - alpha) * output[-1])
    return output


def candle_start(candle: dict) -> datetime:
    # 이 시각은 빗썸 API가 제공하는 일봉 기준 시각(KST)입니다.
    return datetime.fromisoformat(candle["candle_date_time_kst"]).replace(tzinfo=KST)


def session_start(now: datetime, start_hour: int, start_minute: int = 0) -> datetime:
    boundary = now.astimezone(KST).replace(
        hour=start_hour, minute=start_minute, second=0, microsecond=0
    )
    return boundary if now >= boundary else boundary - timedelta(days=1)


def price_text(value: float) -> str:
    return f"{value:,.0f}" if value >= 1000 else f"{value:,.6f}".rstrip("0").rstrip(".")


def markets_all() -> dict[str, str]:
    rows = api_get("/v1/market/all", {"isDetails": "false"})
    return {
        row["market"]: row.get("korean_name") or row["market"]
        for row in rows
        if row.get("market", "").startswith("KRW-")
    }


def detect_day_boundary_hour() -> int:
    # 문서의 오래된 예시나 앱 설정값을 추정하지 않고 실제 최신 일봉 시각을 사용합니다.
    candles = api_get("/v1/candles/days", {"market": "KRW-BTC", "count": 2})
    if not candles:
        raise RuntimeError("KRW-BTC 일봉을 조회할 수 없습니다")
    return candle_start(candles[0]).hour


def update_daily_cache(
    market_names: dict[str, str],
    boundary_hour: int,
    state: dict[str, set[str]],
) -> dict[str, dict]:
    now = datetime.now(KST)
    current_session = session_start(now, boundary_hour)
    expected_last_session = current_session - timedelta(days=1)
    updated = {}
    for i, (market, name) in enumerate(market_names.items(), start=1):
        try:
            candles = api_get("/v1/candles/days", {"market": market, "count": HISTORY_COUNT})
            candles.sort(key=lambda row: row["candle_date_time_kst"])
            completed = [c for c in candles if candle_start(c) < current_session]
            if len(completed) < MIN_COMPLETED_CANDLES:
                continue
            prices = [float(c["trade_price"]) for c in completed]
            ema9, ema20 = calculate_ema(prices, 9), calculate_ema(prices, 20)
            last_start = candle_start(completed[-1])
            prev_spread = ema9[-1] - ema20[-1]
            updated[market] = {
                "name": name,
                "ema9": ema9[-1],
                "ema20": ema20[-1],
                "last_closed_start": last_start,
                "previous_spread": prev_spread,
            }
            # 직전 일봉이 막 마감된 시점의 확정 교차 여부.
            # 과거에 종료된 거래일에 뒤늦게 알림을 보내지 않도록 최근 1거래일에 한정합니다.
            if last_start == expected_last_session and ema9[-2] <= ema20[-2] and prev_spread > 0:
                key_day = last_start.date().isoformat()
                msg = (
                    f"✅ [빗썸 일봉 확정 골든크로스]\n"
                    f"{name} ({market})\n"
                    f"EMA9: {price_text(ema9[-1])}원\n"
                    f"EMA20: {price_text(ema20[-1])}원\n"
                    f"확정 일봉 시작일: {key_day} (KST)\n"
                    f"일봉 마감 후 신호 확인"
                )
                send_once(state, "confirmed", market, key_day, msg)
        except Exception as exc:  # 한 종목 오류가 전체 감시를 멈추지 않도록 함
            log.warning("%s 일봉 갱신 실패: %s", market, exc)
        time.sleep(0.055)  # 공식 API 호출 제한에 여유를 두기 위함
        if i % 50 == 0:
            log.info("일봉 갱신 중: %d / %d", i, len(market_names))
    log.info("일봉 갱신 완료: %d / %d 종목 (최소 %d봉)", len(updated), len(market_names), MIN_COMPLETED_CANDLES)
    return updated


def scan_live(
    daily: dict[str, dict],
    boundary_hour: int,
    state: dict[str, set[str]],
) -> None:
    if not daily:
        return
    markets = list(daily)
    current_session = session_start(datetime.now(KST), boundary_hour)
    current_day = current_session.date().isoformat()
    for start in range(0, len(markets), 200):
        chunk = markets[start:start + 200]
        tickers = api_get("/v1/ticker", {"markets": ",".join(chunk)})
        for ticker in tickers:
            market = ticker.get("market")
            if market not in daily:
                continue
            info = daily[market]
            if info["last_closed_start"] != current_session - timedelta(days=1):
                continue  # 데이터 지연/거래 공백 시 오래된 기준값으로 오탐 방지
            if info["previous_spread"] > 0:
                continue  # 전일 이미 EMA9>EMA20이면 '오늘 신규 교차'가 아님
            current = float(ticker["trade_price"])
            live9 = (2.0 / 10) * current + (1.0 - 2.0 / 10) * info["ema9"]
            live20 = (2.0 / 21) * current + (1.0 - 2.0 / 21) * info["ema20"]
            if live9 > live20:
                msg = (
                    f"🟢 [빗썸 EMA 실시간 잠정 골든크로스]\n"
                    f"{info['name']} ({market})\n"
                    f"현재가: {price_text(current)}원\n"
                    f"EMA9(잠정): {price_text(live9)}원\n"
                    f"EMA20(잠정): {price_text(live20)}원\n"
                    f"확인 시간: {datetime.now(KST):%Y-%m-%d %H:%M} KST\n"
                    f"일봉 마감 전에 신호가 사라질 수 있습니다."
                )
                send_once(state, "live", market, current_day, msg)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    state = load_state()
    boundary_hour = detect_day_boundary_hour()
    log.info("빗썸 최신 일봉 API 기준 시각: KST %02d:00", boundary_hour)
    if not notify(f"🚀 빗썸 EMA 9/20 감시 시작\nKRW 전체 종목 / 1분 간격\n일봉 API 기준: KST {boundary_hour:02d}:00\n실시간 잠정 + 마감 확정 알림"):
        raise SystemExit("텔레그램 연결 실패. config.env의 토큰/채팅 ID를 확인하세요.")
    market_names = markets_all()
    daily = update_daily_cache(market_names, boundary_hour, state)
    last_session = session_start(datetime.now(KST), boundary_hour)
    next_refresh = time.monotonic() + HISTORY_REFRESH_SECONDS
    while True:
        start_time = time.monotonic()
        try:
            now = datetime.now(KST)
            new_session = session_start(now, boundary_hour)
            # 당일 일봉이 바뀌었거나 1시간이 지났으면 일봉과 상장 목록 재조회.
            # 자정/오전 9시 중 어떤 기준이든 API 데이터로 자동 판별합니다.
            if new_session != last_session or time.monotonic() >= next_refresh:
                market_names = markets_all()
                new_daily = update_daily_cache(market_names, boundary_hour, state)
                if new_daily:
                    daily = new_daily
                last_session = new_session
                next_refresh = time.monotonic() + HISTORY_REFRESH_SECONDS
            scan_live(daily, boundary_hour, state)
        except Exception as exc:
            log.exception("감시 주기 오류 (다음 주기에 재시도): %s", exc)
        time.sleep(max(1, POLL_SECONDS - (time.monotonic() - start_time)))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n종료했습니다.")
