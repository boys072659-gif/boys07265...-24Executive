#!/usr/bin/env python3
"""
STEP 4: SJAMES → 메인 프로젝트 데이터 마이그레이션

기능:
  - 5개 테이블 데이터 복사 (SJAMES public.* → 메인 public.exec_*)
  - 페이지네이션으로 PostgREST 1000건 제한 우회
  - 멱등(idempotent): 여러 번 실행해도 중복 없이 안전
  - 진행상황 상세 로그
  - 마이그레이션 검증 (원본 vs 복사본 건수 비교)

사용법:
  python migrate_data.py

필수 환경변수 (또는 아래 상수 직접 수정):
  - SRC_SUPABASE_URL: SJAMES 프로젝트 URL
  - SRC_SUPABASE_KEY: SJAMES service_role key (anon 아님!)
  - DST_SUPABASE_URL: 메인 프로젝트 URL
  - DST_SUPABASE_KEY: 메인 service_role key

⚠️ service_role key 사용 이유:
  - RLS 우회 가능 (임시 정책으로 anon도 가능하지만 안전 우선)
  - dept_passwords 같은 민감 테이블 접근

⚠️ service_role key 가져오는 법:
  Supabase Dashboard → Settings → API → service_role secret
  (anon key 옆에 reveal 버튼 누르면 보임)
  절대 깃허브에 commit 하지 말 것! 환경변수로만 사용.
"""

import os
import sys
import json
import time
import argparse
from urllib.parse import quote

try:
    import httpx
except ImportError:
    print("❌ httpx 모듈이 필요합니다. 설치:")
    print("   pip install httpx")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# 설정 — 환경변수 우선, 없으면 여기 직접 입력
# ═══════════════════════════════════════════════════════════════
SRC_SUPABASE_URL = os.environ.get("SRC_SUPABASE_URL", "https://atfdkfvcokdgfidxgdlo.supabase.co")
SRC_SUPABASE_KEY = os.environ.get("SRC_SUPABASE_KEY", "")  # ← SJAMES service_role key

DST_SUPABASE_URL = os.environ.get("DST_SUPABASE_URL", "https://faxaqxyafsitdfykxgem.supabase.co")
DST_SUPABASE_KEY = os.environ.get("DST_SUPABASE_KEY", "")  # ← 메인 service_role key

PAGE_SIZE = 500  # 한 번에 옮기는 행 수
HTTP_TIMEOUT = 60.0


# ═══════════════════════════════════════════════════════════════
# 테이블 매핑 (SJAMES 원본 → 메인 exec_ 테이블)
# ═══════════════════════════════════════════════════════════════
TABLES = [
    {
        # checklist_items: PK 는 item_key (id 컬럼 없음)
        "src": "checklist_items",
        "dst": "exec_checklist_items",
        "conflict": "item_key",
        "drop_cols": [],
    },
    {
        # checklist_overrides: PK 는 id (TEXT) — 원본 id 그대로 유지해야 함
        # 그래야 SJAMES 클라이언트 코드가 같은 id 로 데이터 찾을 수 있음
        "src": "checklist_overrides",
        "dst": "exec_checklist_overrides",
        "conflict": "id",
        "drop_cols": [],
    },
    # dept_passwords 는 SJAMES 에 테이블 자체가 없어 제외 (확인 완료)
    {
        # dept_telegram: PK 는 dept_id
        "src": "dept_telegram",
        "dst": "exec_dept_telegram",
        "conflict": "dept_id",
        "drop_cols": [],
    },
    {
        # notify_log: PK 는 id (TEXT)
        "src": "notify_log",
        "dst": "exec_notify_log",
        "conflict": "id",
        "drop_cols": [],
    },
]


# ═══════════════════════════════════════════════════════════════
# 공용 헬퍼
# ═══════════════════════════════════════════════════════════════
def log(level, msg):
    icon = {"INFO": "ℹ️ ", "OK": "✅", "WARN": "⚠️ ", "ERR": "❌"}.get(level, "  ")
    print(f"{icon} {msg}", flush=True)


def validate_config():
    """설정 검증 — 잘못된 키로 진행하면 큰일 남."""
    errors = []
    if not SRC_SUPABASE_URL.startswith("https://"):
        errors.append("SRC_SUPABASE_URL 형식 오류")
    if not SRC_SUPABASE_KEY or len(SRC_SUPABASE_KEY) < 50:
        errors.append("SRC_SUPABASE_KEY 누락 또는 잘못됨")
    if not DST_SUPABASE_URL.startswith("https://"):
        errors.append("DST_SUPABASE_URL 형식 오류")
    if not DST_SUPABASE_KEY or len(DST_SUPABASE_KEY) < 50:
        errors.append("DST_SUPABASE_KEY 누락 또는 잘못됨")
    if SRC_SUPABASE_URL == DST_SUPABASE_URL:
        errors.append("SRC 와 DST 가 같은 프로젝트! 절대 안 됨")
    if errors:
        for e in errors:
            log("ERR", e)
        sys.exit(1)
    log("OK", f"SRC: {SRC_SUPABASE_URL}")
    log("OK", f"DST: {DST_SUPABASE_URL}")


def headers_for(key, prefer_resolution=False):
    h = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if prefer_resolution:
        # 충돌 시 merge-duplicates 로 upsert
        h["Prefer"] = "resolution=merge-duplicates,return=minimal"
    return h


def _order_key_for(table):
    """테이블별 정렬 키 (실제 PK 또는 unique 컬럼).
    SJAMES 의 실제 스키마:
      - checklist_items     PK = item_key
      - checklist_overrides PK = id (TEXT)
      - dept_telegram       PK = dept_id
      - notify_log          PK = id (TEXT)
    """
    return {
        "checklist_items":     "item_key",
        "checklist_overrides": "id",
        "dept_telegram":       "dept_id",
        "notify_log":          "id",
    }.get(table, "id")


def fetch_all(url, key, table, page_size=PAGE_SIZE):
    """페이지네이션으로 전체 행 가져오기."""
    all_rows = []
    offset = 0
    order_col = _order_key_for(table)
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        while True:
            r = client.get(
                f"{url}/rest/v1/{table}",
                headers=headers_for(key),
                params={
                    "select": "*",
                    "offset": offset,
                    "limit": page_size,
                    "order": order_col,
                },
            )
            if r.status_code != 200:
                log("ERR", f"{table} 조회 실패: {r.status_code} {r.text[:300]}")
                return None
            chunk = r.json()
            if not chunk:
                break
            all_rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
            if offset > 1_000_000:
                log("ERR", f"{table} 100만행 초과 — 비정상")
                return None
    return all_rows


def upsert_chunk(url, key, table, rows, conflict_col=None):
    """청크 단위로 upsert."""
    if not rows:
        return 0
    params = {}
    if conflict_col:
        params["on_conflict"] = conflict_col
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        r = client.post(
            f"{url}/rest/v1/{table}",
            headers=headers_for(key, prefer_resolution=True),
            params=params,
            json=rows,
        )
    if r.status_code not in (200, 201, 204):
        log("ERR", f"{table} upsert 실패: {r.status_code} {r.text[:500]}")
        return -1
    return len(rows)


def count_rows(url, key, table):
    """행 수 빠르게 카운트 (HEAD + Prefer: count=exact)."""
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        r = client.head(
            f"{url}/rest/v1/{table}",
            headers={
                **headers_for(key),
                "Prefer": "count=exact",
            },
            params={"select": "*"},
        )
    if r.status_code not in (200, 206):
        return -1
    # Content-Range: 0-999/12345 형식
    cr = r.headers.get("content-range", "")
    if "/" in cr:
        try:
            return int(cr.split("/")[-1])
        except ValueError:
            return -1
    return -1


# ═══════════════════════════════════════════════════════════════
# 메인 마이그레이션 로직
# ═══════════════════════════════════════════════════════════════
def migrate_table(t):
    src, dst = t["src"], t["dst"]
    conflict = t["conflict"]
    drop_cols = set(t["drop_cols"])

    log("INFO", f"\n━━━ [{src}] → [{dst}] ━━━")

    # 1) 원본 카운트
    src_count = count_rows(SRC_SUPABASE_URL, SRC_SUPABASE_KEY, src)
    log("INFO", f"원본 행 수: {src_count}")
    if src_count <= 0:
        log("WARN", "원본이 비어있거나 카운트 실패 — 건너뜀")
        return {"src": src, "dst": dst, "src_count": src_count, "copied": 0, "ok": src_count == 0}

    # 2) 데이터 전체 가져오기 (페이지네이션)
    rows = fetch_all(SRC_SUPABASE_URL, SRC_SUPABASE_KEY, src)
    if rows is None:
        log("ERR", f"{src} 가져오기 실패 — 중단")
        return {"src": src, "dst": dst, "src_count": src_count, "copied": 0, "ok": False}
    log("OK", f"{len(rows)} 행 가져옴")

    # 3) drop_cols 제거 (BIGSERIAL id 등은 대상 DB에서 새로 받음)
    if drop_cols:
        for r in rows:
            for c in drop_cols:
                r.pop(c, None)

    # 4) 청크 단위로 upsert
    total_copied = 0
    CHUNK = 200  # POST body 크기 제한 회피
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        result = upsert_chunk(DST_SUPABASE_URL, DST_SUPABASE_KEY, dst, chunk, conflict)
        if result < 0:
            log("ERR", f"{dst} 청크 {i}-{i+CHUNK} 실패 — 중단")
            return {"src": src, "dst": dst, "src_count": src_count, "copied": total_copied, "ok": False}
        total_copied += result
        log("INFO", f"  ... {total_copied}/{len(rows)} 행 복사됨")

    # 5) 대상 카운트 검증
    dst_count = count_rows(DST_SUPABASE_URL, DST_SUPABASE_KEY, dst)
    log("INFO", f"대상 테이블 최종 행 수: {dst_count}")

    return {
        "src": src,
        "dst": dst,
        "src_count": src_count,
        "copied": total_copied,
        "dst_count": dst_count,
        "ok": (conflict is not None and total_copied >= src_count) or (dst_count >= src_count),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="원본 행 수만 확인")
    parser.add_argument("--only", help="특정 테이블만 (예: checklist_overrides)")
    args = parser.parse_args()

    log("INFO", "═══ SJAMES → 메인 데이터 마이그레이션 시작 ═══")
    validate_config()

    if args.dry_run:
        log("INFO", "[DRY-RUN] 원본 행 수만 확인합니다.")
        for t in TABLES:
            if args.only and t["src"] != args.only:
                continue
            c = count_rows(SRC_SUPABASE_URL, SRC_SUPABASE_KEY, t["src"])
            log("INFO", f"  {t['src']}: {c}행")
        return

    results = []
    for t in TABLES:
        if args.only and t["src"] != args.only:
            continue
        try:
            results.append(migrate_table(t))
        except Exception as e:
            log("ERR", f"{t['src']} 예외: {e}")
            results.append({"src": t["src"], "dst": t["dst"], "ok": False, "error": str(e)})

    # 최종 요약
    log("INFO", "\n═══ 마이그레이션 요약 ═══")
    all_ok = True
    for r in results:
        status = "✅" if r.get("ok") else "❌"
        log("INFO", f"{status} {r['src']:25s} → {r['dst']:30s}  "
                     f"src={r.get('src_count','?')} copied={r.get('copied','?')} dst={r.get('dst_count','?')}")
        if not r.get("ok"):
            all_ok = False

    if all_ok:
        log("OK", "🎉 모든 테이블 마이그레이션 성공!")
        log("INFO", "다음 단계: STEP 5 (Storage 파일 복사)")
    else:
        log("ERR", "일부 테이블 실패 — 위 로그 확인 후 재실행 (멱등하므로 안전)")
        sys.exit(1)


if __name__ == "__main__":
    main()
