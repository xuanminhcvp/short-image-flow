#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_multi_sheet_flow.py

Chạy nhiều Google Sheet theo file nguồn (giống style black-auto-python).

Định dạng file sources:
  - Mỗi dòng 1 sheet URL/ID
  - Hoặc: <sheet_url_or_id>|<project_mode>
  - Dòng bắt đầu bằng # sẽ bị bỏ qua

Ví dụ:
  https://docs.google.com/spreadsheets/d/abc/edit#gid=0|black-auto
  1AbCdEfGhIjKlMnOpQrStUvWxYz1234567890|hoa
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from services.sheet_drive_flow_service import SheetFlowConfig, run_sheet_drive_flow_pipeline


def parse_args() -> argparse.Namespace:
    """
    Parse tham số CLI.

    Yêu cầu bắt buộc:
    - sources: file danh sách sheet
    - credentials/token-file: auth Google
    - drive-parent-black-auto: folder Drive cha cho mode black-auto

    Tuỳ chọn:
    - drive-parent-hoa: folder Drive cha cho mode hoa (nếu có dòng |hoa)
    """
    p = argparse.ArgumentParser(description="Chạy nhiều sheet -> Flow -> Drive -> Sheet")
    p.add_argument("--sources", required=True, help="File nguồn kiểu sheet_url|project_mode")
    p.add_argument("--credentials", required=True, help="Credentials OAuth/Service Account")
    p.add_argument("--token-file", required=True, help="Token OAuth file")
    p.add_argument(
        "--drive-parent-black-auto",
        default="root",
        help="Drive folder ID cho mode black-auto. Mặc định: root (My Drive gốc).",
    )
    p.add_argument(
        "--drive-parent-hoa",
        default="root",
        help="Drive folder ID cho mode hoa. Mặc định: root (My Drive gốc).",
    )
    p.add_argument("--workspace-dir", default="scenarios/sheets_pipeline", help="Thư mục local lưu scenario tạm")
    p.add_argument("--video-workers-config", default="config/video_workers.json", help="Config workers cho Flow")
    p.add_argument("--row-start", type=int, default=2, help="Dòng bắt đầu xử lý")
    p.add_argument("--row-end", type=int, default=0, help="Dòng kết thúc, 0 = không giới hạn")
    p.add_argument("--public-link", action="store_true", help="Mở quyền xem công khai cho folder/file Drive output")
    p.add_argument("--use-proxy", action="store_true", help="Bật proxy theo video_workers.json")
    p.add_argument("--scene-timeout-per-prompt-sec", type=int, default=180, help="Timeout mỗi prompt scene (giây)")
    p.add_argument("--scenario-timeout-sec", type=int, default=30 * 60, help="Timeout tối đa cho 1 row/kịch bản (giây)")
    p.add_argument("--scene-min-success-images", type=int, default=120, help="Mục tiêu mềm số ảnh (để retry)")
    p.add_argument("--scene-retry-failed-rounds", type=int, default=2, help="Số vòng retry prompt fail/timeout")
    return p.parse_args()


def parse_sources_file(path: Path) -> list[dict]:
    """
    Parse file sources thành list item:
      {"sheet": "...", "mode": "black-auto" | "hoa"}
    """
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file sources: {path}")

    out: list[dict] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = str(raw or "").strip()
        if not line or line.startswith("#"):
            continue
        sheet = line
        mode = "black-auto"
        if "|" in line:
            left, right = line.split("|", 1)
            sheet = left.strip()
            mode = (right.strip() or "black-auto").lower()
        if not sheet:
            continue
        out.append({"sheet": sheet, "mode": mode})
    return out


def pick_drive_parent_id(mode: str, args: argparse.Namespace) -> str:
    """
    Chọn Drive parent folder ID theo project_mode.
    """
    m = str(mode or "black-auto").strip().lower()
    if m == "hoa":
        return str(args.drive_parent_hoa or "root").strip() or "root"
    return str(args.drive_parent_black_auto or "root").strip() or "root"


def main() -> None:
    args = parse_args()
    items = parse_sources_file(Path(args.sources))
    if not items:
        raise RuntimeError("File sources không có dòng hợp lệ để chạy.")

    summary: list[dict] = []
    total_ok = 0
    total_fail = 0

    print(f"[multi-sheet] Tổng sheet cần chạy: {len(items)}")

    for idx, item in enumerate(items, start=1):
        sheet = item["sheet"]
        mode = item["mode"]
        drive_parent_id = pick_drive_parent_id(mode, args)
        print("\n" + "=" * 80)
        print(f"[multi-sheet] [{idx}/{len(items)}] mode={mode} | sheet={sheet}")
        print("=" * 80)

        cfg = SheetFlowConfig(
            sheet=sheet,
            credentials=str(args.credentials),
            token_file=str(args.token_file),
            drive_output_parent_id=drive_parent_id,
            workspace_dir=str(args.workspace_dir),
            video_workers_config=str(args.video_workers_config),
            use_proxy=bool(args.use_proxy),
            public_link=bool(args.public_link),
            row_start=int(args.row_start),
            row_end=int(args.row_end),
            scene_timeout_per_prompt_sec=int(args.scene_timeout_per_prompt_sec),
            scenario_timeout_sec=int(args.scenario_timeout_sec),
            scene_min_success_images=int(args.scene_min_success_images),
            scene_retry_failed_rounds=int(args.scene_retry_failed_rounds),
        )

        result = run_sheet_drive_flow_pipeline(cfg)
        summary.append(result)
        total_ok += int(result.get("ok", 0) or 0)
        total_fail += int(result.get("fail", 0) or 0)
        print(f"[multi-sheet] Kết quả sheet: ok={result.get('ok', 0)} | fail={result.get('fail', 0)}")

    out = {
        "sheets": len(items),
        "total_ok": total_ok,
        "total_fail": total_fail,
        "results": summary,
    }
    out_path = Path(args.workspace_dir) / "multi_sheet_summary.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 80)
    print(f"[multi-sheet] DONE | total_ok={total_ok} | total_fail={total_fail}")
    print(f"[multi-sheet] Summary: {out_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()
