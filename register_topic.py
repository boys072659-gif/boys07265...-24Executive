#!/usr/bin/env python3
"""
SJAMES 24행정부서 - 새 토픽 ID 자동 등록 스크립트

용도:
  부서별 그룹방의 새 토픽에서 "/register_topic" 입력 시,
  봇이 그 토픽 ID를 잡아 exec_dept_telegram.topic_id를 자동 교체.

실행 방식:
  GitHub Actions의 "토픽 등록 모드"로 수동 실행.
  POLL_MINUTES 동안 폴링하며 명령을 처리한 뒤 자동 종료.

식별 방식:
  메시지의 chat.id(그룹 ID)로 exec_dept_telegram에서 부서를 찾아
  해당 부서의 topic_id를 메시지가 온 토픽 ID로 갱신.

응답:
  성공/실패/이미 동일을 그 토픽에 답글로 표시 (확신을 주는 UX).
"""
import os
import sys
import json
import time
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.parse import quote
from urllib.error import HTTPError, URLError

# ─────────────────────────────────────────────────────
# 환경변수 (GitHub Actions Secrets로 주입)
# ─────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
BOT_TOKEN    = os.environ.get('TELEGRAM_BOT_TOKEN', '')
POLL_MINUTES = int(os.environ.get('POLL_MINUTES', '5'))  # 폴링 지속 시간 (분)

if not (SUPABASE_URL and SUPABASE_KEY and BOT_TOKEN):
    print('❌ 환경변수 누락: SUPABASE_URL / SUPABASE_KEY / TELEGRAM_BOT_TOKEN')
    sys.exit(1)

COMMAND = '/register_topic'
TG_API = f'https://api.telegram.org/bot{BOT_TOKEN}'
SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation',
}

print(f'[시작] {datetime.now().isoformat()}')
print(f'[설정] 명령어: {COMMAND}, 폴링 시간: {POLL_MINUTES}분')


# ─────────────────────────────────────────────────────
# 도우미 함수
# ─────────────────────────────────────────────────────
def _make_sb_request(url, method='GET', body=None):
    """Supabase REST API 요청 헬퍼 — add_header 방식으로 안정적으로 헤더 설정."""
    data = json.dumps(body).encode('utf-8') if body is not None else None
    req = Request(url, data=data, method=method)
    for k, v in SB_HEADERS.items():
        req.add_header(k, v)
    try:
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except HTTPError as e:
        body_text = e.read().decode('utf-8', errors='replace')
        raise Exception(f'Supabase {method} {url}\n  → HTTP {e.code}: {body_text[:300]}')


def http_get(url, timeout=35):
    return _make_sb_request(url, 'GET')


def http_patch(url, body):
    return _make_sb_request(url, 'PATCH', body)


def tg_call(method, params=None, timeout=35):
    """텔레그램 API 호출. params는 GET 쿼리스트링으로."""
    url = f'{TG_API}/{method}'
    if params:
        url += '?' + '&'.join(f'{k}={quote(str(v))}' for k, v in params.items())
    try:
        with urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        return {'ok': False, 'error_code': e.code, 'description': body}
    except URLError as e:
        return {'ok': False, 'description': str(e)}


def tg_send_reply(chat_id, topic_id, text, reply_to=None):
    """답글로 메시지 전송 (사용자가 확인하기 좋도록)."""
    params = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML',
    }
    if topic_id:
        params['message_thread_id'] = topic_id
    if reply_to:
        params['reply_to_message_id'] = reply_to
    return tg_call('sendMessage', params)


# ─────────────────────────────────────────────────────
# Webhook 사용 중이면 폴링이 안 되므로 안내
# ─────────────────────────────────────────────────────
wh = tg_call('getWebhookInfo')
if wh.get('ok') and wh.get('result', {}).get('url'):
    print(f'⚠️ 현재 봇에 Webhook이 설정돼 있어 폴링이 막힙니다: {wh["result"]["url"]}')
    print('  → 이 스크립트만 실행 중에는 일시적으로 deleteWebhook이 필요할 수 있어요.')
    print('  → 지금은 안전하게 종료합니다. (운영 webhook을 임의로 끄지 않기 위해서)')
    sys.exit(0)


# ─────────────────────────────────────────────────────
# 부서 매핑 캐시: chat_id → {dept_id, dept_name, current_topic_id}
# ─────────────────────────────────────────────────────
def load_dept_map():
    url = f'{SUPABASE_URL}/rest/v1/exec_dept_telegram?select=dept_id,chat_id,topic_id,enabled,note'
    rows = http_get(url)
    m = {}
    for r in rows:
        cid = str(r.get('chat_id') or '').strip()
        if not cid:
            continue
        # note가 있으면 부서명으로, 없으면 dept_id 사용
        dept_name = (r.get('note') or '').strip() or r['dept_id']
        m[cid] = {
            'dept_id': r['dept_id'],
            'dept_name': dept_name,
            'current_topic_id': r.get('topic_id'),
            'enabled': r.get('enabled', True),
        }
    return m


def update_topic_id(dept_id, new_topic_id):
    url = f'{SUPABASE_URL}/rest/v1/exec_dept_telegram?dept_id=eq.{quote(dept_id)}'
    return http_patch(url, {'topic_id': str(new_topic_id)})


# ─────────────────────────────────────────────────────
# 메시지 처리
# ─────────────────────────────────────────────────────
def handle_update(update, dept_map):
    msg = update.get('message') or update.get('channel_post')
    if not msg:
        return None

    text = (msg.get('text') or '').strip()
    if not text:
        return None

    # 명령어 매칭 (봇 멘션 포함도 허용: /register_topic@executive_24_bot)
    first_word = text.split()[0].lower()
    if first_word != COMMAND and not first_word.startswith(COMMAND + '@'):
        return None

    chat = msg.get('chat') or {}
    chat_id = str(chat.get('id') or '')
    topic_id = msg.get('message_thread_id')
    message_id = msg.get('message_id')

    if not chat_id:
        return None

    print(f'\n[명령] chat_id={chat_id}, topic_id={topic_id}, from={msg.get("from",{}).get("first_name")}')

    # 토픽 안에서 보낸 게 아닌 경우
    if not topic_id:
        tg_send_reply(chat_id, None,
            '⚠️ 이 명령은 <b>새로 만든 토픽 안에서</b> 입력해주세요.\n'
            '(그룹 메인이 아닌, 등록하고 싶은 토픽으로 들어가서 다시 입력)',
            reply_to=message_id)
        return None

    # 그룹이 등록된 부서인지
    info = dept_map.get(chat_id)
    if not info:
        tg_send_reply(chat_id, topic_id,
            f'⚠️ 이 그룹(<code>{chat_id}</code>)은 부서 명단에 없습니다.\n'
            f'먼저 봇 관리 패널에서 이 그룹의 chat_id로 부서를 등록해주세요.',
            reply_to=message_id)
        return None

    # 이미 같은 topic_id면 안내만
    current = str(info['current_topic_id'] or '')
    if current == str(topic_id):
        tg_send_reply(chat_id, topic_id,
            f'ℹ️ <b>{info["dept_name"]}</b>의 토픽 ID는 이미 <code>{topic_id}</code>로 등록돼 있어요.\n'
            f'추가 작업이 필요 없습니다.',
            reply_to=message_id)
        return None

    # 업데이트 시도
    try:
        update_topic_id(info['dept_id'], topic_id)
        tg_send_reply(chat_id, topic_id,
            f'✅ <b>{info["dept_name"]}</b>의 알림 토픽이 이 토픽으로 교체되었습니다.\n'
            f'  · 이전 토픽: <code>{current or "없음"}</code>\n'
            f'  · 새 토픽: <code>{topic_id}</code>\n'
            f'앞으로 모든 자동 알림은 이 토픽으로 발송됩니다.',
            reply_to=message_id)
        # 캐시도 갱신
        info['current_topic_id'] = topic_id
        print(f'  → 업데이트 성공: {info["dept_name"]} topic_id={current} → {topic_id}')
        return info['dept_id']
    except Exception as e:
        print(f'  ❌ 업데이트 실패: {e}')
        tg_send_reply(chat_id, topic_id,
            f'❌ 등록 실패: <code>{str(e)[:200]}</code>\n관리자에게 문의해주세요.',
            reply_to=message_id)
        return None


# ─────────────────────────────────────────────────────
# 메인 폴링 루프
# ─────────────────────────────────────────────────────
def main():
    print('\n[1/3] 부서 매핑 로드...')
    dept_map = load_dept_map()
    print(f'  → 등록된 부서 {len(dept_map)}개 (chat_id 기준)')

    print('\n[2/3] 폴링 시작 (Ctrl+C로 중단 가능)...')
    deadline = time.time() + POLL_MINUTES * 60
    last_update_id = 0
    registered = []

    # 시작 시점에 쌓여있던 옛 update를 한 번 비우고 시작 (오래된 명령 오작동 방지)
    init = tg_call('getUpdates', {'timeout': 0, 'limit': 100})
    if init.get('ok') and init.get('result'):
        last_update_id = init['result'][-1]['update_id']
        print(f'  → 이전 update {len(init["result"])}건 스킵 (last_update_id={last_update_id})')

    poll_count = 0
    while time.time() < deadline:
        poll_count += 1
        remaining = int(deadline - time.time())

        result = tg_call('getUpdates', {
            'offset': last_update_id + 1,
            'timeout': min(25, max(1, remaining)),  # long-polling
            'allowed_updates': '["message"]',
        })

        if not result.get('ok'):
            print(f'  ⚠️ getUpdates 실패: {result.get("description")}')
            time.sleep(3)
            continue

        updates = result.get('result', [])
        if updates:
            print(f'  [폴 {poll_count}] update {len(updates)}건 수신')

        for upd in updates:
            last_update_id = max(last_update_id, upd['update_id'])
            dept_id = handle_update(upd, dept_map)
            if dept_id and dept_id not in registered:
                registered.append(dept_id)

    print('\n[3/3] 폴링 종료')
    print(f'  → 총 폴링: {poll_count}회')
    print(f'  → 새로 등록된 부서: {len(registered)}개')
    for d in registered:
        info = dept_map.get(next((cid for cid, v in dept_map.items() if v['dept_id'] == d), ''), {})
        print(f'    · {info.get("dept_name", d)}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n[중단] 사용자 요청')
    except Exception as e:
        print(f'\n❌ 오류: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
