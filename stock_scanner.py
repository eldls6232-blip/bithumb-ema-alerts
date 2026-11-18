#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""KRX KOSPI/KOSDAQ daily EMA 9/20 cross + 100 point technical candidate alert.

Requires the official KRX OPEN API key with both stock daily trade services approved.
Only derived candidate summaries are sent to Telegram; raw KRX data are not persisted.
Not financial advice; scores are an unvalidated heuristic, not probabilities.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

KST = ZoneInfo('Asia/Seoul')
BASE = 'https://data-dbg.krx.co.kr/svc/apis/sto/'
MARKETS = {'KOSPI': 'stk_bydd_trd', 'KOSDAQ': 'ksq_bydd_trd'}
STATE_PATH = Path('stock_alert_state.json')
MIN_TURNOVER = 500_000_000  # KRW per day
HISTORY_CALENDAR_DAYS = 330  # > 200 trading sessions in usual market years
MAX_WORKERS = 6
LOG = logging.getLogger('krx-stock-alert')
REQUEST_LOCK = threading.Lock()
NEXT_REQUEST_AT = 0.0


def throttle_krx() -> None:
    """Share a cautious five-requests-per-second gate among fetch workers."""
    global NEXT_REQUEST_AT
    with REQUEST_LOCK:
        now = time.monotonic()
        delay = max(0.0, NEXT_REQUEST_AT - now)
        if delay:
            time.sleep(delay)
        NEXT_REQUEST_AT = time.monotonic() + 0.2


def to_number(value) -> float:
    try:
        return float(str(value).replace(',', '').strip())
    except (ValueError, TypeError):
        return float('nan')


def stock_code(isin: str) -> str:
    code = str(isin).strip()
    if re.fullmatch(r'\d{6}', code):
        return code
    # Common Korean equity ISIN: KR7 + 6-character stock short code + checksum section.
    if re.fullmatch(r'KR\w{10}', code) and code[3:9].isdigit():
        return code[3:9]
    return code


def is_common_stock(name: str) -> bool:
    """Conservative exclusions; actual market coverage depends on KRX equity feeds."""
    name = str(name).strip()
    if not name or '스팩' in name or 'SPAC' in name.upper():
        return False
    if re.search(r'(우|우B|우C|우\d+|우\(전환\))$', name):
        return False
    return True


def krx_day(market: str, on_date: date, key: str) -> tuple[str, date, list[dict]]:
    """Fetch one market/date. Fail closed on provider errors, never treat errors as holidays."""
    endpoint = MARKETS[market]
    url = BASE + endpoint
    headers = {'AUTH_KEY': key, 'Accept': 'application/json'}
    params = {'basDd': on_date.strftime('%Y%m%d')}
    for attempt in range(4):
        try:
            throttle_krx()
            response = requests.get(url, params=params, headers=headers, timeout=25)
            if response.status_code in (401, 403):
                raise RuntimeError(f'{market} KRX 인증/서비스 이용 승인 오류 HTTP {response.status_code}')
            if response.status_code == 429:
                raise RuntimeError('KRX API 호출 한도 초과(429)')
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or not isinstance(body.get('OutBlock_1'), list):
                # Unexpected response may be a permission error in HTTP 200.
                raise RuntimeError(f'{market} {on_date} KRX 응답 형식 오류/서비스 미승인: {str(body)[:150]}')
            return (market, on_date, body['OutBlock_1'])
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError, ValueError) as exc:
            if attempt == 3:
                raise RuntimeError(f'{market} {on_date} 조회 실패: {type(exc).__name__}') from exc
            time.sleep(1.5 * (attempt + 1))
    raise AssertionError('unreachable')


def most_recent_session(today: date, key: str) -> tuple[date, dict[str, list[dict]]]:
    # The official daily stock API normally refreshes prior-day data the next morning.
    # Never use today's still-open trading bar.
    start = today - timedelta(days=1)
    for days_back in range(11):
        d = start - timedelta(days=days_back)
        if d.weekday() >= 5:
            continue
        results = {}
        for market in MARKETS:
            _, _, items = krx_day(market, d, key)
            results[market] = items
        if all(len(x) > 0 for x in results.values()):
            if days_back > 5:
                raise RuntimeError(f'최신 KRX 자료가 너무 오래됨: {d.isoformat()}')
            return d, results
        if any(len(x) > 0 for x in results.values()):
            raise RuntimeError(f'{d} 일부 시장 데이터만 제공됨 - 분석 중단')
    raise RuntimeError('최근 일별 KRX 데이터 없음 - API 승인 또는 제공시점 확인')


def load_state() -> dict:
    try:
        value = json.loads(STATE_PATH.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except FileNotFoundError:
        return {}


def save_state(latest: date, sent_symbols: list[str]) -> None:
    state = {'last_scan_date': latest.isoformat(), 'notified': sent_symbols}
    temp = STATE_PATH.with_suffix('.tmp')
    temp.write_text(json.dumps(state, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    temp.replace(STATE_PATH)


def fetch_history(last_date: date, key: str, recent: dict[str, list[dict]]) -> dict[tuple[str, str], list]:
    """Collect all listed stocks; do not store full market candles in a public repository."""
    data: dict[tuple[str, str], list] = defaultdict(list)

    def consume(market: str, d: date, items: list[dict]) -> None:
        for item in items:
            ident = str(item.get('ISU_CD', '')).strip()
            name = str(item.get('ISU_NM', '')).strip()
            if not ident or not is_common_stock(name):
                continue
            o, h, l, c, v, tv = [to_number(item.get(k)) for k in (
                'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC',
                'ACC_TRDVOL', 'ACC_TRDVAL')]
            if not all(math.isfinite(val) for val in (o, h, l, c, v, tv)):
                continue
            if min(o, h, l, c) <= 0 or v <= 0 or tv <= 0:
                continue
            data[(market, ident)].append((d, o, h, l, c, v, tv, name))

    for m, rows in recent.items():
        consume(m, last_date, rows)

    tasks = [(d, market) for offset in range(1, HISTORY_CALENDAR_DAYS + 1)
             for d in [last_date - timedelta(days=offset)] if d.weekday() < 5
             for market in MARKETS]
    LOG.info('과거 일별 데이터 요청: %s회 / %s일', len(tasks), HISTORY_CALENDAR_DAYS)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(krx_day, market, d, key) for d, market in tasks]
        for idx, future in enumerate(as_completed(futures), 1):
            market, day, rows = future.result()  # stop on any fetch error
            consume(market, day, rows)
            if idx % 80 == 0:
                LOG.info('데이터 조회 %d/%d회', idx, len(tasks))

    return data


def adx_frame(df: pd.DataFrame, n: int = 14) -> tuple[pd.Series, pd.Series, pd.Series]:
    high = df['high']
    low = df['low']
    close = df['close']
    up = high.diff()
    down = -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    true_range = pd.concat([
        high - low, (high - close.shift()).abs(), (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    # Wilder-style EWM for ADX and ATR; score is heuristic.
    atr = true_range.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / n, adjust=False, min_periods=n).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / n, adjust=False, min_periods=n).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return adx, plus_di, atr


def score_stock(bars: list, market: str, ident: str, latest: date) -> dict | None:
    bars = sorted(bars, key=lambda x: x[0])
    if len(bars) < 145 or bars[-1][0] != latest:
        return None
    df = pd.DataFrame(bars, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'name'])
    df = df.drop_duplicates(subset='date', keep='last').sort_values('date').reset_index(drop=True)
    if len(df) < 145:
        return None
    closes = df['close'].astype(float)
    if df['turnover'].iloc[-1] < MIN_TURNOVER:
        return None
    # Ignore large jumps that might indicate unadjusted stock split/reverse split.
    ratios = closes.pct_change().tail(66).dropna()
    if (ratios.abs() > 0.40).any():
        return None
    ema9 = closes.ewm(span=9, adjust=False, min_periods=9).mean()
    ema20 = closes.ewm(span=20, adjust=False, min_periods=20).mean()
    ema60 = closes.ewm(span=60, adjust=False, min_periods=60).mean()
    if not (ema9.iloc[-2] <= ema20.iloc[-2] and ema9.iloc[-1] > ema20.iloc[-1]):
        return None

    diff = closes.diff()
    gain = diff.clip(lower=0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    loss = (-diff.clip(upper=0)).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    latest_rsi = float(rsi.iloc[-1])
    if not math.isfinite(latest_rsi) or latest_rsi >= 75:
        return None

    ema12 = closes.ewm(span=12, adjust=False).mean()
    ema26 = closes.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    adx, plus_di, atr = adx_frame(df)
    score = 20  # newly confirmed EMA 9/20 golden cross
    reasons = ['EMA9/20 일봉 골든크로스(+20)']

    if ema20.iloc[-1] > ema60.iloc[-1] and ema20.iloc[-1] > ema20.iloc[-6]:
        score += 15
        reasons.append('EMA20 > EMA60·상승 추세(+15)')
    if 45 <= latest_rsi <= 65:
        score += 15
        reasons.append(f'RSI {latest_rsi:.1f} 적정(+15)')
    elif 40 <= latest_rsi < 75:
        score += 8
        reasons.append(f'RSI {latest_rsi:.1f} 주의(+8)')
    if hist.iloc[-1] > 0 and hist.iloc[-1] > hist.iloc[-2]:
        score += 15
        reasons.append('MACD 모멘텀 개선(+15)')
    a = float(adx.iloc[-1])
    p = float(plus_di.iloc[-1])
    if math.isfinite(a) and math.isfinite(p) and a >= 25 and p >= 20:
        score += 10
        reasons.append(f'ADX {a:.1f} 추세 강도(+10)')
    vol_avg = df['volume'].iloc[-21:-1].mean()
    vol_ratio = float(df['volume'].iloc[-1] / vol_avg) if vol_avg > 0 else 0
    if vol_ratio >= 1.5:
        score += 15
        reasons.append(f'거래량 20일평균 {vol_ratio:.2f}배(+15)')
    elif vol_ratio >= 1.2:
        score += 8
        reasons.append(f'거래량 20일평균 {vol_ratio:.2f}배(+8)')
    atr_pct = 100 * float(atr.iloc[-1]) / float(closes.iloc[-1])
    if 1.0 <= atr_pct <= 6.0:
        score += 10
        reasons.append(f'ATR/가격 {atr_pct:.1f}% (+10)')
    elif 0 < atr_pct <= 9.0:
        score += 5
        reasons.append(f'ATR/가격 {atr_pct:.1f}% (+5)')
    if score < 80:
        return None
    return dict(market=market, symbol=stock_code(ident), name=str(df['name'].iloc[-1]),
                score=score, close=float(closes.iloc[-1]), rsi=latest_rsi, adx=a,
                vol_ratio=vol_ratio, atr_pct=atr_pct,
                turnover=float(df['turnover'].iloc[-1]), reasons=reasons)


def telegram_send(token: str, chat_id: str, text: str) -> None:
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    response = requests.post(url, json={'chat_id': chat_id, 'text': text,
                                        'disable_web_page_preview': True}, timeout=20)
    try:
        resp = response.json()
    except ValueError:
        resp = {}
    if response.status_code != 200 or not resp.get('ok'):
        # Never log the token or URL.
        raise RuntimeError(f'텔레그램 전송 실패: HTTP {response.status_code}, {str(resp.get("description", ""))[:120]}')


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    token = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat = os.environ.get('TELEGRAM_CHAT_ID', '').strip()
    if not token or not chat:
        raise RuntimeError('TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID Secrets가 필요합니다')
    if os.environ.get('TEST_MODE', '').strip().lower() == 'true' or '--test' in sys.argv:
        telegram_send(token, chat, '✅ KOSPI·KOSDAQ EMA 골든크로스 후보 알림 연결 테스트 성공!\n실제 시장 분석은 KRX API 승인 후 작동합니다.')
        LOG.info('텔레그램 테스트 전송 성공')
        return
    key = os.environ.get('KRX_API_KEY', '').strip()
    if not key:
        raise RuntimeError('KRX_API_KEY GitHub Secret이 없습니다. KRX OPEN API에서 인증키 발급+2개 서비스 승인 필요')

    today = datetime.now(KST).date()
    latest, recent = most_recent_session(today, key)
    state = load_state()
    if state.get('last_scan_date') == latest.isoformat():
        LOG.info('이미 분석된 장 마감일: %s, 중복 알림 방지 종료', latest)
        return
    LOG.info('최신 일봉 날짜: %s / KOSPI %d / KOSDAQ %d 종목',
             latest, len(recent['KOSPI']), len(recent['KOSDAQ']))
    history = fetch_history(latest, key, recent)
    candidates = []
    for (market, ident), bars in history.items():
        signal = score_stock(bars, market, ident, latest)
        if signal:
            candidates.append(signal)
    candidates.sort(key=lambda r: (r['score'], r['turnover']), reverse=True)
    LOG.info('분석 대상 %d 종목 / 80점 이상 신규 골든크로스 %d개', len(history), len(candidates))
    for c in candidates:
        reason_lines = '\n'.join('• ' + r for r in c['reasons'])
        message = (f'📈 {c["market"]} 일봉 EMA9/20 매수 검토 후보\n'
                   f'{c["name"]} ({c["symbol"]}) | {latest.isoformat()} 종가\n'
                   f'기술점수: {c["score"]}/100 (확률 아님)\n'
                   f'종가: {c["close"]:,.0f}원 | 거래대금: {c["turnover"]/100_000_000:,.1f}억원\n'
                   f'RSI: {c["rsi"]:.1f} | ADX: {c["adx"]:.1f} | 거래량: {c["vol_ratio"]:.2f}배\n'
                   f'{reason_lines}\n'
                   '⚠️ 지표 기반 참고용. 뉴스·공시·유동성·손실위험을 확인하세요.\n'
                   '출처: 한국거래소 통계정보')
        telegram_send(token, chat, message)
        time.sleep(0.15)
    save_state(latest, [f'{c["market"]}:{c["symbol"]}' for c in candidates])
    LOG.info('종료: 후보 %d개 / 상태파일 저장. 신규 후보가 없으면 텔레그램 알림도 없습니다.', len(candidates))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        LOG.error('스캔 실패: %s', error)
        sys.exit(1)
