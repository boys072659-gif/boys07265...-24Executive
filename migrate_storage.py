#!/usr/bin/env python3
"""
STEP 5: SJAMES Storage 파일 마이그레이션

기능:
  - SJAMES 의 attachments 버킷 → 메인의 exec_attachments 버킷으로 전체 복사
  - 폴더 구조 보존 (payload 안의 file path가 그대로 유효하려면 필수)
  - 멱등 (이미 복사한 파일은 건너뜀)
  - 실패 시 재시도 가능
  - 마지막에 payload 안의 file path 가 실제 옮긴 파일과 일치하는지 검증

사용법:
  python migrate_storage.py              # 실제 실행
  python migrate_storage.py --dry-run    # 옮길 파일 목록만 확인
  python migrate_storage.py --verify     # 마이그레이션 후 검증만

⚠️ 사전 조건:
  - STEP 2 SQL 실행 완료 (exec_* 테이블 존재)
  - STEP 3 버킷 생성 완료 (exec_attachments)
  - STEP 4 데이터 마이그레이션 완료 (payload 검증 위해)
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path

try:
    import httpx
except ImportError:
    print("❌ httpx 모듈이 필요합니다. 설치:")
    print("   pip install httpx")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# 설정
# ═══════════════════════════════════════════════════════════════
SRC_SUPABASE_URL = os.environ.get("SRC_SUPABASE_URL", "https://atfdkfvcokdgfidxgdlo.supabase.co")
SRC_SUPABASE_KEY = os.environ.get("SRC_SUPABASE_KEY", "")
SRC_BUCKET = "attachments"

DST_SUPABASE_URL = os.environ.get("DST_SUPABASE_URL", "https://faxaqxyafsitdfykxgem.supabase.co")
DST_SUPABASE_KEY = os.environ.get("DST_SUPABASE_KEY", "")
DST_BUCKET = "exec_attachments"

HTTP_TIMEOUT = 120.0

# 로컬 임시 저장 폴더
TMP_DIR = Path("/tmp/sjames_migration_storage")


# ═══════════════════════════════════════════════════════════════
# 헬퍼
# ═══════════════════════════════════════════════════════════════
def log(level, msg):
    icon = {"INFO": "ℹ️ ", "OK": "✅", "WARN": "⚠️ ", "ERR": "❌", "SKIP": "⏭️ "}.get(level, "  ")
    print(f"{icon} {msg}", flush=True)


def headers_for(key):
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }


def list_bucket_recursive(url, key, bucket, prefix=""):
    """버킷 안의 모든 파일을 재귀적으로 나열.
    Supabase Storage list API 는 한 폴더 단위로 동작.
    """
    files = []
    folders_to_process = [prefix]

    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        while folders_to_process:
            current = folders_to_process.pop(0)
            offset = 0
            while True:
                r = client.post(
                    f"{url}/storage/v1/object/list/{bucket}",
                    headers={**headers_for(key), "Content-Type": "application/json"},
                    json={
                        "prefix": current,
                        "limit": 100,
                        "offset": offset,
                        "sortBy": {"column": "name", "order": "asc"},
                    },
                )
                if r.status_code != 200:
                    log("ERR", f"버킷 list 실패 prefix={current}: {r.status_code} {r.text[:200]}")
                    break
                items = r.json()
                if not items:
                    break

                for item in items:
                    name = item.get("name", "")
                    if not name:
                        continue
                    full_path = f"{current}/{name}".lstrip("/") if current else name
                    # 폴더 vs 파일 판정: metadata 없거나 id 없으면 폴더
                    is_folder = item.get("id") is None and item.get("metadata") is None
                    if is_folder:
                        folders_to_process.append(full_path)
                    else:
                        files.append({
                            "path": full_path,
                            "size": (item.get("metadata") or {}).get("size", 0),
                            "name": name,
                        })

                if len(items) < 100:
                    break
                offset += 100
    return files


def download_file(url, key, bucket, path, local_path):
    """Storage 에서 파일 다운로드."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        r = client.get(
            f"{url}/storage/v1/object/{bucket}/{path}",
            headers=headers_for(key),
        )
    if r.status_code != 200:
        log("ERR", f"다운로드 실패 {path}: {r.status_code}")
        return False
    local_path.write_bytes(r.content)
    return True


def upload_file(url, key, bucket, path, local_path):
    """Storage 에 파일 업로드 (이미 있으면 덮어쓰기)."""
    with open(local_path, "rb") as f:
        data = f.read()
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        # upsert=true 로 이미 있으면 덮어씀 (멱등 보장)
        r = client.post(
            f"{url}/storage/v1/object/{bucket}/{path}",
            headers={
                **headers_for(key),
                "Content-Type": "application/octet-stream",
                "x-upsert": "true",
            },
            content=data,
        )
    if r.status_code not in (200, 201):
        log("ERR", f"업로드 실패 {path}: {r.status_code} {r.text[:200]}")
        return False
    return True


def file_exists_in_dst(url, key, bucket, path):
    """대상 버킷에 이미 있는지 확인 (멱등성 위해)."""
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        r = client.get(
            f"{url}/storage/v1/object/info/{bucket}/{path}",
            headers=headers_for(key),
        )
    return r.status_code == 200


# ═══════════════════════════════════════════════════════════════
# 메인 — Storage 마이그레이션
# ═══════════════════════════════════════════════════════════════
def migrate_storage(dry_run=False):
    log("INFO", "═══ Storage 파일 마이그레이션 ═══")
    log("INFO", f"SRC: {SRC_SUPABASE_URL} / bucket={SRC_BUCKET}")
    log("INFO", f"DST: {DST_SUPABASE_URL} / bucket={DST_BUCKET}")

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # 1) 원본 버킷 전체 파일 목록
    log("INFO", "원본 버킷 스캔 중...")
    src_files = list_bucket_recursive(SRC_SUPABASE_URL, SRC_SUPABASE_KEY, SRC_BUCKET)
    log("OK", f"원본 파일 {len(src_files)}개 발견")
    total_size = sum(f.get("size", 0) for f in src_files)
    log("INFO", f"총 용량: {total_size/1024/1024:.2f} MB")

    if dry_run:
        log("INFO", "[DRY-RUN] 파일 목록 (최대 20개):")
        for f in src_files[:20]:
            log("INFO", f"  - {f['path']} ({f['size']} bytes)")
        if len(src_files) > 20:
            log("INFO", f"  ... 외 {len(src_files)-20}개")
        return

    # 2) 각 파일을 다운로드 → 업로드 (이미 있으면 건너뜀)
    success = 0
    skipped = 0
    failed = 0
    for i, f in enumerate(src_files, 1):
        path = f["path"]
        try:
            # 멱등성: 대상에 이미 있으면 스킵
            if file_exists_in_dst(DST_SUPABASE_URL, DST_SUPABASE_KEY, DST_BUCKET, path):
                skipped += 1
                if i % 10 == 0 or i == len(src_files):
                    log("SKIP", f"[{i}/{len(src_files)}] {path} (이미 존재)")
                continue

            local_path = TMP_DIR / path.replace("/", os.sep)
            if not download_file(SRC_SUPABASE_URL, SRC_SUPABASE_KEY, SRC_BUCKET, path, local_path):
                failed += 1
                continue
            if not upload_file(DST_SUPABASE_URL, DST_SUPABASE_KEY, DST_BUCKET, path, local_path):
                failed += 1
                continue

            # 로컬 임시 파일 즉시 삭제 (디스크 절약)
            local_path.unlink(missing_ok=True)

            success += 1
            if i % 10 == 0 or i == len(src_files):
                log("OK", f"[{i}/{len(src_files)}] {path}")
        except Exception as e:
            log("ERR", f"{path} 예외: {e}")
            failed += 1

    # 결과 요약
    log("INFO", "\n═══ Storage 마이그레이션 요약 ═══")
    log("OK" if failed == 0 else "WARN", f"성공: {success}개")
    log("INFO", f"스킵(이미 존재): {skipped}개")
    if failed > 0:
        log("ERR", f"실패: {failed}개 — 재실행하면 스킵된 것 외에는 다시 시도함 (멱등)")
        return False
    return True


# ═══════════════════════════════════════════════════════════════
# payload 안의 파일 경로 검증
# ═══════════════════════════════════════════════════════════════
def verify_payload_paths():
    """exec_checklist_overrides.payload 안의 planFiles / resultFiles 경로가
    실제 대상 버킷에 있는지 확인.
    
    누락된 게 있으면 어떤 행/어떤 path 가 빠졌는지 정확히 알려줌.
    """
    log("INFO", "\n═══ payload 파일 경로 검증 ═══")

    # exec_checklist_overrides 전체 가져오기
    all_rows = []
    offset = 0
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        while True:
            r = client.get(
                f"{DST_SUPABASE_URL}/rest/v1/exec_checklist_overrides",
                headers=headers_for(DST_SUPABASE_KEY),
                params={"select": "id,target_id,dept_id,payload", "offset": offset, "limit": 500, "order": "id"},
            )
            if r.status_code != 200:
                log("ERR", f"exec_checklist_overrides 조회 실패: {r.status_code}")
                return False
            chunk = r.json()
            if not chunk: break
            all_rows.extend(chunk)
            if len(chunk) < 500: break
            offset += 500

    log("INFO", f"검증 대상 행 수: {len(all_rows)}")

    # payload 안의 파일 경로 추출
    referenced_paths = []  # [(row_id, target_id, dept_id, field, path)]
    for r in all_rows:
        payload = r.get("payload") or {}
        if not isinstance(payload, dict):
            continue
        for field in ("planFiles", "resultFiles"):
            files = payload.get(field) or []
            if not isinstance(files, list):
                continue
            for fobj in files:
                if isinstance(fobj, dict):
                    p = fobj.get("path") or fobj.get("name") or ""
                    if p:
                        referenced_paths.append({
                            "row_id":    r.get("id"),
                            "target_id": r.get("target_id"),
                            "dept_id":   r.get("dept_id"),
                            "field":     field,
                            "path":      p,
                        })

    log("INFO", f"payload 가 참조하는 파일 경로: {len(referenced_paths)}개")

    # 대상 버킷의 모든 파일 목록
    log("INFO", "대상 버킷 스캔 중...")
    dst_files = list_bucket_recursive(DST_SUPABASE_URL, DST_SUPABASE_KEY, DST_BUCKET)
    dst_paths = {f["path"] for f in dst_files}
    log("OK", f"대상 버킷 파일: {len(dst_paths)}개")

    # 매칭 검증
    missing = [ref for ref in referenced_paths if ref["path"] not in dst_paths]
    if not missing:
        log("OK", "🎉 모든 payload 파일 경로가 대상 버킷에 존재함!")
        return True

    log("WARN", f"누락된 파일: {len(missing)}개")
    for m in missing[:30]:  # 처음 30개만
        log("WARN", f"  row#{m['row_id']} ({m['dept_id']}/{m['target_id']}) {m['field']}: {m['path']}")
    if len(missing) > 30:
        log("WARN", f"  ... 외 {len(missing)-30}개")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="복사할 파일 목록만 출력")
    parser.add_argument("--verify", action="store_true", help="복사 안 하고 payload 경로만 검증")
    args = parser.parse_args()

    # 설정 검증
    if not SRC_SUPABASE_KEY or not DST_SUPABASE_KEY:
        log("ERR", "환경변수 SRC_SUPABASE_KEY, DST_SUPABASE_KEY 필요")
        sys.exit(1)
    if SRC_SUPABASE_URL == DST_SUPABASE_URL:
        log("ERR", "SRC == DST 면 안 됩니다!")
        sys.exit(1)

    if args.verify:
        ok = verify_payload_paths()
        sys.exit(0 if ok else 1)

    ok = migrate_storage(dry_run=args.dry_run)
    if not args.dry_run and ok:
        log("INFO", "\nStorage 마이그레이션 완료. 이어서 payload 경로 검증:")
        verify_payload_paths()


if __name__ == "__main__":
    main()
