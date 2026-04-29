#!/usr/bin/env python3
"""
SJAMES 24부서 연간계획 - 텔레그램 자동 알림
실행: 매일 KST 09:00 (GitHub Actions cron)
조건:
  - 매월 1일 (월간 시작 알림)
  - 매월 15일 (중간 점검 알림)
  - 매월 마지막날 -1일 (마감 임박 알림)
"""
import os
import sys
import json
import re
from datetime import datetime, timedelta
import calendar
from urllib.request import Request, urlopen
from urllib.parse import quote
from urllib.error import HTTPError, URLError

# ─────────────────────────────────────────────────────
# 환경 변수 (GitHub Actions에서 주입)
# ─────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
APP_URL = os.environ.get('APP_URL', '')  # 예: https://계정.github.io/sjames-checklist/
FORCE_TYPE = os.environ.get('FORCE_TYPE', '')  # 수동 테스트용: 'monthly', 'midmonth', 'deadline'

if not all([SUPABASE_URL, SUPABASE_KEY, BOT_TOKEN]):
    print('❌ 환경변수 누락: SUPABASE_URL / SUPABASE_KEY / TELEGRAM_BOT_TOKEN')
    sys.exit(1)

# KST 타임존 (UTC+9)
KST = datetime.utcnow() + timedelta(hours=9)
TODAY = KST.date()
print(f'[{TODAY}] 실행 시작 (KST {KST.strftime("%H:%M")})')

# ─────────────────────────────────────────────────────
# 오늘이 어떤 알림 날짜인지 판별
# ─────────────────────────────────────────────────────
def determine_notify_type():
    if FORCE_TYPE in ('monthly', 'midmonth', 'deadline'):
        print(f'[FORCE] 수동 발송 모드: {FORCE_TYPE}')
        return FORCE_TYPE

    day = TODAY.day
    last_day = calendar.monthrange(TODAY.year, TODAY.month)[1]

    if day == 1:
        return 'monthly'      # 매월 1일: 월간 알림
    if day == 15:
        return 'midmonth'     # 매월 15일: 중간 점검
    if day == last_day - 1:
        return 'deadline'     # 마감 하루 전
    return None

NOTIFY_TYPE = determine_notify_type()
if not NOTIFY_TYPE:
    print(f'[건너뜀] 오늘({TODAY.day}일)은 알림 발송일이 아님')
    sys.exit(0)

print(f'[알림 종류] {NOTIFY_TYPE}')

# ─────────────────────────────────────────────────────
# Supabase REST API 헬퍼
# ─────────────────────────────────────────────────────
def sb_get(table, params=''):
    url = f'{SUPABASE_URL}/rest/v1/{table}{("?"+params) if params else ""}'
    req = Request(url, headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}'
    })
    try:
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except HTTPError as e:
        print(f'[Supabase 오류] {table}: {e.code} {e.read()}')
        return []
    except URLError as e:
        print(f'[네트워크 오류] {table}: {e}')
        return []

def sb_post(table, body):
    url = f'{SUPABASE_URL}/rest/v1/{table}'
    req = Request(url, method='POST', data=json.dumps(body).encode('utf-8'), headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    })
    try:
        with urlopen(req, timeout=30) as r:
            return True
    except HTTPError as e:
        print(f'[로그 저장 실패] {e.code}')
        return False

# ─────────────────────────────────────────────────────
# 데이터 로드
# ─────────────────────────────────────────────────────
print('[1/4] 부서 텔레그램 정보 로드...')
dept_telegram = sb_get('dept_telegram', 'enabled=eq.true')
if not dept_telegram:
    print('❌ 활성화된 부서 텔레그램 설정이 없습니다.')
    sys.exit(0)
print(f'  → 활성 부서 {len(dept_telegram)}개')

print('[2/4] 부서/항목 데이터 로드...')
overrides = sb_get('checklist_overrides', 'limit=1000')
items = sb_get('checklist_items', 'limit=2000')
print(f'  → overrides {len(overrides)}건, items {len(items)}건')

# 이미 발송했는지 확인
print('[3/4] 발송 로그 확인...')
log_id_template = f'_{NOTIFY_TYPE}_{TODAY.isoformat()}'
sent_logs = sb_get('notify_log', f'sent_at=gte.{TODAY.isoformat()}T00:00:00')
sent_dept_ids = set(l['dept_id'] for l in sent_logs if l.get('notify_type') == NOTIFY_TYPE)
print(f'  → 오늘 이미 발송된 부서: {len(sent_dept_ids)}개')

# items를 dict로
items_by_key = {}
for it in items:
    items_by_key[it['item_key']] = it

# overrides를 dept_id별 detail로 정리
detail_by_item_id = {}
for ov in overrides:
    if ov.get('kind') == 'detail' and ov.get('target_id'):
        detail_by_item_id[ov['target_id']] = ov.get('payload') or {}

# ─────────────────────────────────────────────────────
# 24부서 기본 데이터 (HTML과 동일하게 유지)
# ※ HTML 변경 시 이 부분도 같이 갱신해야 함
# ─────────────────────────────────────────────────────
DEPTS = [
    # 내무
    ('d03', '03 내무부'), ('d31', '3-1 자문회'), ('d32', '3-2 장년회'), ('d33', '3-3 부녀회'),
    ('d34', '3-4 청년회'), ('d35', '3-5 학생회'), ('d36', '3-6 유년회'), ('d37', '3-7 국제부'),
    # 행정
    ('d04', '04 기획부'), ('d05', '05 재정부'), ('d06', '06 교육부'), ('d07', '07 신학부'),
    ('d08', '08 해외선교부'), ('d09', '09 전도부'), ('d14', '14 섭외부'), ('d15', '15 국내선교부'),
    # 문화
    ('d10', '10 문화부'), ('d11', '11 출판부'), ('d12', '12 정보통신부'), ('d13', '13 찬양부'),
    ('d20', '20 체육부'), ('d21', '21 홍보부'), ('d22', '22 보건후생복지부'), ('d23', '23 봉사교통부'),
    ('d19', '19 건설부'),
]
DEPT_NAME_MAP = dict(DEPTS)

# ─────────────────────────────────────────────────────
# 부서별 연간 월별 계획 항목 수집
# 항목은 overrides의 'add' 액션 + (HTML 원본 항목)으로 구성됨
# 여기서는 overrides에 있는 항목만으로 처리
# (HTML 원본 항목 데이터는 클라이언트에 있어 서버에서는 모름)
# ─────────────────────────────────────────────────────
print('[4/4] 부서별 그달 항목 정리...')

# 항목 ID → (deptId, sectionId, title) 매핑을 overrides에서 추출
# detail의 경우 dept_id, section_id가 있음
detail_meta = {}
for ov in overrides:
    if ov.get('kind') == 'detail' and ov.get('target_id'):
        detail_meta[ov['target_id']] = {
            'dept_id': ov.get('dept_id'),
            'section_id': ov.get('section_id'),
        }

# add 액션에서 항목 정보 (사용자가 추가한 항목)
added_items = {}
for ov in overrides:
    if ov.get('kind') == 'item' and ov.get('action') == 'add' and ov.get('payload'):
        p = ov.get('payload') or {}
        item_id = p.get('id') or ov.get('target_id')
        if item_id:
            added_items[item_id] = {
                'id': item_id,
                'dept_id': ov.get('dept_id'),
                'section_id': ov.get('section_id'),
                't': p.get('t', ''),
            }

# 수정된 항목
edited_items = {}
for ov in overrides:
    if ov.get('kind') == 'item' and ov.get('action') == 'update' and ov.get('payload'):
        p = ov.get('payload') or {}
        item_id = ov.get('target_id')
        if item_id:
            edited_items[item_id] = p

# 삭제된 항목
deleted_items = set()
for ov in overrides:
    if ov.get('kind') == 'item' and ov.get('action') == 'delete':
        deleted_items.add(ov.get('target_id'))

# 월 추출 (항목 텍스트 첫 부분에서)
MONTH_PATTERNS = [
    re.compile(r'^(\d{1,2})월'),
    re.compile(r'^(\d{1,2})~(\d{1,2})월'),
    re.compile(r'\b(\d{1,2})월'),
]

def extract_months(text):
    """항목 텍스트에서 월 번호 추출 (여러 월 가능)"""
    if not text:
        return []
    months = set()
    # 우선 "X월" 또는 "X~Y월" 형태
    m = re.match(r'^(\d{1,2})~(\d{1,2})월', text.strip())
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        for i in range(a, b+1):
            if 1 <= i <= 12:
                months.add(i)
        return sorted(months)
    m = re.match(r'^(\d{1,2})월', text.strip())
    if m:
        n = int(m.group(1))
        if 1 <= n <= 12:
            months.add(n)
    # 본문에서 추가 월 검색 ("매달", "분기별" 같은 키워드 처리는 1차에서는 생략)
    return sorted(months)

# 부서별 그달 항목 목록 만들기
TARGET_MONTH = TODAY.month

dept_monthly = {dept_id: {'completed': [], 'pending': []} for dept_id, _ in DEPTS}

# 추가된 항목 + 원본 항목 모두 처리
all_items = {}
all_items.update(added_items)
# detail_meta로부터 dept_id 알 수 있는 항목만
for item_id, meta in detail_meta.items():
    if item_id in deleted_items:
        continue
    if item_id not in all_items:
        # detail만 있고 원본 항목 정보가 없는 경우
        # → HTML 원본 항목 (제목을 모름)
        all_items[item_id] = {
            'id': item_id,
            'dept_id': meta.get('dept_id'),
            'section_id': meta.get('section_id'),
            't': '',  # 빈 제목
        }

# 수정 적용
for item_id, edit in edited_items.items():
    if item_id in all_items:
        all_items[item_id].update({k: v for k, v in edit.items() if k in ('t', 'target', 'unit')})
    else:
        # 원본만 있던 항목이 수정된 경우 - 제목 가져옴
        all_items[item_id] = {
            'id': item_id,
            'dept_id': edit.get('dept_id'),
            'section_id': edit.get('section_id'),
            't': edit.get('t', ''),
        }

# 삭제 제외
for item_id in deleted_items:
    all_items.pop(item_id, None)

# 부서별 / 월별 분류
for item_id, item in all_items.items():
    dept_id = item.get('dept_id')
    if dept_id not in dept_monthly:
        continue
    # event 타입만 (section_id가 's3' 또는 비슷한 패턴이 event)
    section_id = item.get('section_id') or ''
    # section_id가 None이면 이벤트 섹션으로 가정 (detail이 있는 항목은 대개 event)
    title = item.get('t') or ''
    months = extract_months(title)
    if TARGET_MONTH not in months:
        # 제목에 월이 명시되지 않은 경우 - 일단 제외
        continue
    detail = detail_by_item_id.get(item_id, {})
    is_completed = detail.get('completed') is True
    entry = {
        'title': title,
        'completed': is_completed,
        'completedBy': detail.get('completedBy'),
        'completedLink': detail.get('completedLink'),
        'hasPlan': bool((detail.get('plan') or '').strip()),
        'hasFiles': bool(detail.get('files')),
    }
    if is_completed:
        dept_monthly[dept_id]['completed'].append(entry)
    else:
        dept_monthly[dept_id]['pending'].append(entry)

# ─────────────────────────────────────────────────────
# 메시지 생성 및 발송
# ─────────────────────────────────────────────────────
def build_message(dept_id, dept_name, monthly_data, notify_type):
    completed = monthly_data['completed']
    pending = monthly_data['pending']
    total = len(completed) + len(pending)

    if total == 0:
        return None  # 그달 항목이 없으면 발송 안 함

    pct = int(len(completed) / total * 100) if total else 0

    # 알림 종류별 헤더
    if notify_type == 'monthly':
        header = f'📅 *[{dept_name}]* {TARGET_MONTH}월 계획 알림'
        intro = f'이번 달 계획을 안내드립니다.\n진행 부탁드립니다 🙏'
    elif notify_type == 'midmonth':
        header = f'⏰ *[{dept_name}]* {TARGET_MONTH}월 중간 점검 ({TODAY.day}일)'
        intro = f'이번 달도 절반이 지났습니다.\n진행 상황을 확인해주세요!'
    else:  # deadline
        last_day = calendar.monthrange(TODAY.year, TODAY.month)[1]
        header = f'🚨 *[{dept_name}]* {TARGET_MONTH}월 마감 임박! (D-1)'
        intro = f'내일({TARGET_MONTH}/{last_day})이 이번 달 마감입니다.\n미완료 항목 보고 부탁드립니다!'

    # 진행률
    progress_bar = '█' * (pct // 10) + '░' * (10 - pct // 10)
    progress_line = f'📊 진행률: {progress_bar} *{pct}%* ({len(completed)}/{total})'

    parts = [header, '', intro, '', progress_line, '']

    # 완료 항목
    if completed:
        parts.append(f'✅ *완료된 항목 ({len(completed)})*')
        for c in completed:
            link = f' [🔗]({c["completedLink"]})' if c.get('completedLink') else ''
            by = f' _{c["completedBy"]}_' if c.get('completedBy') else ''
            parts.append(f'  ✓ {c["title"]}{link}{by}')
        parts.append('')

    # 미완료 항목
    if pending:
        parts.append(f'⬜ *미완료 항목 ({len(pending)})*')
        for p in pending:
            extra = []
            if p.get('hasPlan'): extra.append('📋')
            if p.get('hasFiles'): extra.append('📎')
            extra_str = (' ' + ''.join(extra)) if extra else ''
            parts.append(f'  ☐ {p["title"]}{extra_str}')
        parts.append('')

    # 점검표 링크
    if APP_URL:
        parts.append(f'🔗 점검표: {APP_URL}')

    return '\n'.join(parts)

def send_telegram(chat_id, text, topic_id=None):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': True,
    }
    # 포럼/주제 그룹용 message_thread_id
    if topic_id:
        try:
            payload['message_thread_id'] = int(topic_id)
        except (ValueError, TypeError):
            pass
    body = json.dumps(payload).encode('utf-8')
    req = Request(url, method='POST', data=body, headers={
        'Content-Type': 'application/json'
    })
    try:
        with urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
            return data.get('ok', False), None
    except HTTPError as e:
        err = e.read().decode('utf-8', errors='replace')
        return False, f'{e.code}: {err}'
    except URLError as e:
        return False, str(e)

def log_send(dept_id, status, message_count):
    log_id = f'{dept_id}_{NOTIFY_TYPE}_{TODAY.isoformat()}'
    sb_post('notify_log', {
        'id': log_id,
        'dept_id': dept_id,
        'notify_type': NOTIFY_TYPE,
        'sent_at': KST.isoformat(),
        'message_count': message_count,
        'status': status,
    })

# ─────────────────────────────────────────────────────
# 부서별 발송
# ─────────────────────────────────────────────────────
print('\n========== 발송 시작 ==========')
success_count = 0
skip_count = 0
fail_count = 0

for tg in dept_telegram:
    dept_id = tg.get('dept_id')
    chat_id = tg.get('chat_id')
    topic_id = tg.get('topic_id')  # 포럼/주제 그룹용 (없으면 None)
    if not dept_id or not chat_id:
        continue

    dept_name = DEPT_NAME_MAP.get(dept_id, dept_id)

    # 중복 발송 방지
    if dept_id in sent_dept_ids:
        print(f'[{dept_name}] 이미 발송됨 (건너뜀)')
        skip_count += 1
        continue

    monthly_data = dept_monthly.get(dept_id, {'completed': [], 'pending': []})
    message = build_message(dept_id, dept_name, monthly_data, NOTIFY_TYPE)

    if not message:
        print(f'[{dept_name}] {TARGET_MONTH}월 항목 없음 (건너뜀)')
        skip_count += 1
        continue

    ok, err = send_telegram(chat_id, message, topic_id)
    msg_count = len(monthly_data['completed']) + len(monthly_data['pending'])

    topic_info = f' (토픽 {topic_id})' if topic_id else ''
    if ok:
        print(f'[{dept_name}]{topic_info} ✅ 발송 완료 ({msg_count}개 항목)')
        log_send(dept_id, 'success', msg_count)
        success_count += 1
    else:
        print(f'[{dept_name}] ❌ 발송 실패: {err}')
        log_send(dept_id, f'failed: {err[:100]}', msg_count)
        fail_count += 1

print('\n========== 결과 ==========')
print(f'✅ 성공: {success_count}개 부서')
print(f'⏭️  건너뜀: {skip_count}개 부서')
print(f'❌ 실패: {fail_count}개 부서')
sys.exit(0 if fail_count == 0 else 1)
