"""GitHub Actions용 빗썸 EMA 9/20 일봉 알림: 한 번 조회하고 종료.

기존 bithumb_ema_bot.py의 지표 계산 및 텔레그램 전송 함수를 재사용합니다.
환경변수 TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID 필수.
"""
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bithumb_ema_bot as bot

KST = timezone(timedelta(hours=9))
HEARTBEAT_FILE = Path("workflow_heartbeat.txt")


def heartbeat() -> None:
    """공개 저장소 예약 워크플로 60일 미활동 자동중지를 피하도록 주기적으로 기록 갱신."""
    today = datetime.now(KST).date()
    try:
        previous = datetime.fromisoformat(HEARTBEAT_FILE.read_text(encoding="utf-8").strip()).date()
    except (FileNotFoundError, ValueError, OSError):
        previous = today - timedelta(days=30)
    if (today - previous).days >= 25:
        HEARTBEAT_FILE.write_text(today.isoformat() + "\n", encoding="utf-8")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch":
        if not bot.notify("✅ 빗썸 EMA 9/20 GitHub Actions 알림 연결 테스트 성공\n원화마켓 일봉 골든크로스를 주기적으로 감시합니다."):
            return 1
    state = bot.load_state()
    boundary = bot.detect_day_boundary_hour()
    names = bot.markets_all()
    logging.info("빗썸 KRW 종목 %s개 감시 시작, 일봉 시작시간 %02d:00 KST", len(names), boundary)
    if not names:
        logging.error("KRW 마켓 목록이 비어 있습니다")
        return 1
    history = bot.update_daily_cache(names, boundary, state)
    if not history:
        logging.error("일봉 분석 데이터가 없습니다")
        return 1
    bot.scan_live(history, boundary, state)
    heartbeat()
    logging.info("감시 완료: %d종목", len(history))
    return 0


if __name__ == "__main__":
    sys.exit(main())
