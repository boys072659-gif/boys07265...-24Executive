#!/usr/bin/env python3
"""
SJAMES 봉사기록부 일일 리포트 — Edge Function 자동 호출
  - 07:00 start 모드 / 20:00 end 모드
  - MODE 환경변수로 구분 (GitHub Actions에서 주입)
  - 관리자 지정 공휴일(attend_holidays) 제외
"""

import os, sys, json, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
MODE         = os.environ.get('MODE', 'end')   # 'start' or 'end'

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print('❌ 환경변수 누락: SUPABASE_URL / SUPABASE_KEY')
    sys.exit(1)

KST = timezone(timedelta(hours=9))
now_kst   = datetime.now(KST)
today_str = now_kst.strftime('%Y-%m-%d')
DOW_NAMES = ['월','화','수','목','금','토','일']
print(f'[봉사기록부] 오늘: {today_str} ({DOW_NAMES[now_kst.weekday()]}요일) MODE={MODE}')

# ── 1) 공휴일 체크 ─────────────────────────────────────────
try:
    req = urllib.request.Request(
        f'{SUPABASE_URL}/rest/v1/rpc/list_attend_holidays_public',
        method='POST',
        headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}',
                 'Content-Type': 'application/json'},
        data=b'{}'
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        holidays = {h.get('holiday_date') for h in (json.loads(resp.read()) or []) if isinstance(h, dict)}
        if today_str in holidays:
            print(f'[건너뜀] {today_str} 관리자 지정 공휴일')
            sys.exit(0)
        print(f'[공휴일 체크] 통과 ({len(holidays)}개 등록, 오늘 해당 없음)')
except Exception as e:
    print(f'[경고] 공휴일 조회 오류: {e} — 그래도 발송 진행')

# ── 2) Edge Function 호출 ──────────────────────────────────
edge_url = f'{SUPABASE_URL}/functions/v1/attend-daily-report'
payload  = json.dumps({'cron': True, 'mode': MODE}).encode()
print(f'[발송] {edge_url} mode={MODE}')

try:
    req = urllib.request.Request(
        edge_url, method='POST',
        headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}',
                 'Content-Type': 'application/json'},
        data=payload
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode())
        print(f'[완료] HTTP {resp.status} version={result.get("version")} sent={result.get("sent")} failed={result.get("failed")}')
        if result.get('failed', 0) > 0:
            print(f'[실패 상세] {json.dumps(result.get("results", []), ensure_ascii=False)[:500]}')
        sys.exit(1 if result.get('failed', 0) > 0 else 0)
except urllib.error.HTTPError as e:
    print(f'[실패] HTTP {e.code}: {e.read().decode()[:500]}')
    sys.exit(1)
except Exception as e:
    print(f'[실패] {e}')
    sys.exit(1)
