#!/usr/bin/env python3
"""
SJAMES 봉사기록부 일일 리포트 — Edge Function 자동 호출
- 평일(월~금)만 발송
- 관리자가 attend_holidays에 정한 공휴일은 제외 (list_attend_holidays_public RPC 사용)
- GitHub Actions cron으로 매일 KST 08:00에 실행
"""

import os
import sys
import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone


# ─────────────────────────────────────────────────────────
# 환경변수
# ─────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print('❌ 환경변수 누락: SUPABASE_URL / SUPABASE_KEY')
    sys.exit(1)


# ─────────────────────────────────────────────────────────
# 1) KST 오늘 날짜 + 요일 판정
# ─────────────────────────────────────────────────────────
KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST)
today_str = now_kst.strftime('%Y-%m-%d')
dow = now_kst.weekday()  # 0=월, 6=일
DOW_NAMES = ['월', '화', '수', '목', '금', '토', '일']

print(f'[봉사기록부 일일 리포트] 오늘: {today_str} ({DOW_NAMES[dow]}요일)')

# 주말 체크
if dow >= 5:  # 5=토, 6=일
    print(f'[건너뜀] 주말 — 리포트 발송 안 함')
    sys.exit(0)


# ─────────────────────────────────────────────────────────
# 2) 관리자가 정한 공휴일 체크 (list_attend_holidays_public RPC)
# ─────────────────────────────────────────────────────────
rpc_url = f'{SUPABASE_URL}/rest/v1/rpc/list_attend_holidays_public'
req = urllib.request.Request(
    rpc_url,
    method='POST',
    headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
    },
    data=b'{}'
)

try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
        holiday_dates = {h.get('holiday_date') for h in (data or []) if isinstance(h, dict)}
        if today_str in holiday_dates:
            print(f'[건너뜀] 오늘({today_str})은 관리자 지정 공휴일')
            sys.exit(0)
        print(f'[공휴일 체크] 통과 ({len(holiday_dates)}개 공휴일 등록됨, 오늘 해당 없음)')
except urllib.error.HTTPError as e:
    body = e.read().decode('utf-8', errors='replace')[:200]
    print(f'[경고] 공휴일 조회 실패: {e.code} {body}')
    print('       그래도 발송 진행 (안전한 쪽으로 — 평일이라 발송)')
except Exception as e:
    print(f'[경고] 공휴일 조회 오류: {e} — 그래도 발송 진행')


# ─────────────────────────────────────────────────────────
# 3) Edge Function 호출 (attend-daily-report)
# ─────────────────────────────────────────────────────────
edge_url = f'{SUPABASE_URL}/functions/v1/attend-daily-report'
print(f'[발송] {edge_url} 호출 중...')

req = urllib.request.Request(
    edge_url,
    method='POST',
    headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
    },
    data=b'{}'
)

try:
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = resp.read().decode('utf-8')
        print(f'[성공] HTTP {resp.status}')
        print(f'결과: {result[:1000]}')
except urllib.error.HTTPError as e:
    body = e.read().decode('utf-8', errors='replace')
    print(f'[실패] HTTP {e.code}: {body[:1000]}')
    sys.exit(1)
except Exception as e:
    print(f'[실패] {e}')
    sys.exit(1)

print('[완료]')
