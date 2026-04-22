from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from models.flow_settings import FlowSettings
from models.image_job import ImageJob
from models.worker_config import WorkerConfig
from services.flow_scene_generate_service import generate_scene_images_from_job
from services.flow_settings_service import (
    apply_flow_generation_settings_panel,
    load_flow_ui_settings,
)
from services.prompt_service import SCENARIO_IMAGE_FILE, parse_image_prompts_file
from services.worker_pool_service import WorkerPool


@dataclass
class SheetFlowConfig:
    """
    Cấu hình cho pipeline Sheet -> Flow -> Drive.

    Giải thích dễ hiểu:
    - `sheet`: link hoặc ID Google Sheet chứa danh sách job.
    - `credentials`: file credentials OAuth/Service Account.
    - `token_file`: nơi lưu token OAuth sau khi login lần đầu.
    - `drive_output_parent_id`: folder Drive cha để tạo folder kết quả ảnh cho từng dòng.
    - `workspace_dir`: nơi lưu tạm scenario local.
    - `video_workers_config`: file config worker để mở Chrome profile chạy Flow.
    """

    sheet: str
    credentials: str
    token_file: str
    drive_output_parent_id: str
    workspace_dir: str = "scenarios/sheets_pipeline"
    video_workers_config: str = "config/video_workers.json"
    use_proxy: bool = False
    public_link: bool = True

    # Mapping cột trên Google Sheet
    col_prompt_folder: str = "Prompt tạo ảnh"
    col_title: str = "Tiêu đề"
    col_output_folder: str = "Folder ảnh"

    # Giới hạn vùng đọc
    row_start: int = 2
    row_end: int = 0
    max_rows: int = 2000
    range_columns: str = "A:AZ"

    # Tên file prompt cần tải từ Drive folder
    drive_prompt_filename: str = "image_prompts.txt"

    # Retry Google API
    google_retry_attempts: int = 3
    google_retry_sleep_sec: float = 1.2

    # Cấu hình scene pipeline (áp dụng cả khi chạy multi-sheet):
    # - Hết scenario_timeout_sec thì chốt ảnh hiện có, không fail vì thiếu target.
    scene_timeout_per_prompt_sec: int = 180
    scenario_timeout_sec: int = 30 * 60
    scene_min_success_images: int = 120  # mục tiêu mềm để retry, không phải điều kiện fail cứng
    scene_retry_failed_rounds: int = 2


def _log(msg: str) -> None:
    """Log ngắn gọn, dễ đọc để theo dõi pipeline."""
    print(f"[sheet-flow] {msg}", flush=True)


def _retry_google_call(fn, attempts: int = 3, sleep_sec: float = 1.2):
    """
    Retry nhẹ cho call Google API khi lỗi tạm thời.

    Request:
    - Gọi `fn()` (thường là `.execute()`).
    Response:
    - Trả về kết quả ngay nếu thành công.
    - Nếu fail, retry tối đa `attempts` lần rồi raise lỗi cuối cùng.
    """
    import time

    last_exc = None
    max_try = max(1, int(attempts or 1))
    delay = max(0.2, float(sleep_sec or 0.2))
    for i in range(1, max_try + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if i >= max_try:
                break
            _log(f"Google API lỗi tạm thời, retry {i + 1}/{max_try} sau {delay:.1f}s: {exc}")
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


def _extract_sheet_id(raw: str) -> str:
    """Tách spreadsheet ID từ URL hoặc nhận ID thuần."""
    text = str(raw or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", text)
    return m.group(1) if m else text


def _extract_drive_folder_id(raw: str) -> str:
    """Tách Drive folder ID từ URL hoặc nhận ID thuần."""
    text = str(raw or "").strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", text)
    if m:
        return m.group(1)
    if re.match(r"^[a-zA-Z0-9_-]{20,}$", text):
        return text
    return ""


def _is_drive_link_or_id(value: str) -> bool:
    """
    Kiểm tra chuỗi có phải link/ID Drive folder hợp lệ không.

    Mục tiêu:
    - Chỉ chạy khi cột "Prompt tạo ảnh" thực sự có link/ID Drive.
    - Tránh coi text thường là prompt link rồi báo lỗi giả.
    """
    text = str(value or "").strip()
    if not text:
        return False
    if "drive.google.com" in text and "/folders/" in text:
        return True
    return bool(re.match(r"^[a-zA-Z0-9_-]{20,}$", text))


def _a1_to_col_index(label: str) -> int:
    """A1 label -> 0-based index."""
    text = re.sub(r"[^A-Z]", "", str(label or "").upper())
    v = 0
    for ch in text:
        v = v * 26 + (ord(ch) - 64)
    return max(0, v - 1)


def _col_index_to_a1(idx: int) -> str:
    """0-based index -> A1 label."""
    n = int(idx) + 1
    out = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def _find_header_row(rows: list[list[str]], col_name: str) -> int:
    """Tìm dòng header có chứa tên cột cần dùng."""
    target = str(col_name or "").strip()
    for i, row in enumerate(rows):
        for c in row:
            if str(c).strip() == target:
                return i
    return 0


def _find_col(header: list[str], name: str) -> int | None:
    """Tìm index cột theo tên hiển thị."""
    target = str(name or "").strip()
    for i, c in enumerate(header):
        if str(c).strip() == target:
            return i
    return None


def _sanitize_name(text: str, fallback: str) -> str:
    """Chuẩn hóa tên file/folder an toàn."""
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(text or "").strip()).strip("_")
    return cleaned[:120] if cleaned else fallback


def _cleanup_local_output_images(output_dir: Path) -> int:
    """
    Dọn ảnh cũ trong output của một row trước khi generate mới.

    Mục tiêu:
    - Tránh cộng dồn ảnh từ lần chạy trước (gây upload quá nhiều file).
    - Chỉ xóa file ảnh output, không đụng prompt/config.
    """
    exts = {".png", ".jpg", ".jpeg", ".webp"}
    deleted = 0
    if not output_dir.exists():
        return deleted
    for p in output_dir.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() not in exts:
            continue
        try:
            p.unlink()
            deleted += 1
        except Exception:
            # Nếu xóa lỗi thì bỏ qua, bước generate vẫn thử chạy tiếp.
            pass
    return deleted


def _build_zip_from_images(image_paths: list[Path], zip_path: Path) -> Path:
    """
    Nén toàn bộ ảnh thành 1 file zip duy nhất để upload nhanh hơn.
    """
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for img in image_paths:
            if not img.exists() or not img.is_file():
                continue
            # Lưu theo basename để khi giải nén không bị path local dài.
            zf.write(img, arcname=img.name)
    return zip_path


def _build_google_services(credentials_path: Path, token_path: Path):
    """
    Tạo Google Sheets + Drive service từ credentials.

    Hỗ trợ:
    - OAuth desktop (installed/web)
    - Service Account
    """
    try:
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account
        from google.oauth2.credentials import Credentials as UserCredentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError(
            "Thiếu thư viện Google API. Cài: pip install google-api-python-client google-auth google-auth-oauthlib"
        ) from exc

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    raw = json.loads(credentials_path.read_text(encoding="utf-8"))
    is_oauth = isinstance(raw, dict) and ("installed" in raw or "web" in raw)

    if is_oauth:
        creds: Any = None
        if token_path.exists():
            try:
                creds = UserCredentials.from_authorized_user_file(str(token_path), scopes)
            except Exception:
                creds = None
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), scopes)
            creds = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")
    else:
        creds = service_account.Credentials.from_service_account_file(
            str(credentials_path), scopes=scopes
        )

    sheets_svc = build("sheets", "v4", credentials=creds)
    drive_svc = build("drive", "v3", credentials=creds)
    return sheets_svc, drive_svc


def _read_sheet_rows_with_hidden_links(
    sheets_svc: Any,
    spreadsheet_id: str,
    tab_name: str,
    columns: str,
    max_rows: int,
    retry_attempts: int = 3,
    retry_sleep_sec: float = 1.2,
) -> tuple[list[list[str]], dict[tuple[int, int], str]]:
    """
    Đọc text + hyperlink ẩn từ sheet.

    Request gửi đi:
    - values.get để lấy text hiển thị.
    - spreadsheets.get(fields=...) để lấy hyperlink thật trong ô.

    Response nhận về:
    - rows: dữ liệu text theo dòng/cột.
    - hidden_links: map (row_idx, col_idx) -> URL ẩn trong ô.
    """
    parts = str(columns or "A:AZ").upper().split(":", 1)
    c_start = _a1_to_col_index(parts[0])
    c_end = _a1_to_col_index(parts[1] if len(parts) > 1 else parts[0])
    c_end = max(c_start, c_end)
    r_end = max(2, int(max_rows or 2000))
    rng = f"'{tab_name}'!{_col_index_to_a1(c_start)}1:{_col_index_to_a1(c_end)}{r_end}"

    rows_resp = _retry_google_call(
        lambda: sheets_svc.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=rng)
        .execute(),
        attempts=retry_attempts,
        sleep_sec=retry_sleep_sec,
    )
    rows: list[list[str]] = rows_resp.get("values", [])

    raw_cells = _retry_google_call(
        lambda: sheets_svc.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            ranges=[rng],
            fields=(
                "sheets/data/rowData/values/hyperlink,"
                "sheets/data/rowData/values/userEnteredValue,"
                "sheets/data/rowData/values/textFormatRuns/format/link/uri"
            ),
        )
        .execute(),
        attempts=retry_attempts,
        sleep_sec=retry_sleep_sec,
    )
    hidden_links: dict[tuple[int, int], str] = {}
    row_data = raw_cells.get("sheets", [{}])[0].get("data", [{}])[0].get("rowData", [])
    for r_i, row_item in enumerate(row_data):
        for c_i, cell in enumerate((row_item or {}).get("values", []) or []):
            hl = str((cell or {}).get("hyperlink", "")).strip()
            if hl:
                hidden_links[(r_i, c_i)] = hl
                continue
            uv = (cell or {}).get("userEnteredValue", {}) or {}
            formula = str(uv.get("formulaValue", "")).strip()
            m = re.search(r'HYPERLINK\("([^"]+)"', formula, flags=re.IGNORECASE)
            if m:
                hidden_links[(r_i, c_i)] = m.group(1).strip()
                continue
            for run in (cell or {}).get("textFormatRuns", []) or []:
                uri = str((((run or {}).get("format", {}) or {}).get("link", {}) or {}).get("uri", "")).strip()
                if uri:
                    hidden_links[(r_i, c_i)] = uri
                    break
    return rows, hidden_links


def _download_text_file_from_drive_folder(
    drive_svc: Any,
    folder_link_or_id: str,
    target_filename: str,
    retry_attempts: int = 3,
    retry_sleep_sec: float = 1.2,
) -> str:
    """
    Tải file txt từ Drive folder.

    Request gửi đi:
    1) files.list để liệt kê file trong folder.
    2) files.get_media để tải nội dung txt.

    Response nhận về:
    - Nội dung text của file target_filename.
    """
    from googleapiclient.http import MediaIoBaseDownload
    import io

    folder_id = _extract_drive_folder_id(folder_link_or_id)
    if not folder_id:
        raise ValueError(f"Không tách được folder ID từ '{folder_link_or_id}'")

    files_resp = _retry_google_call(
        lambda: drive_svc.files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,name,mimeType)",
            pageSize=100,
        )
        .execute(),
        attempts=retry_attempts,
        sleep_sec=retry_sleep_sec,
    )
    files = files_resp.get("files", [])
    target = None
    for f in files:
        if str(f.get("name", "")).lower() == str(target_filename or "").lower():
            target = f
            break
    if not target:
        raise FileNotFoundError(
            f"Không tìm thấy '{target_filename}' trong folder {folder_id}"
        )

    _log(
        f"Tải prompt file từ Drive: folder_id={folder_id} | "
        f"filename={target_filename} | file_id={target['id']}"
    )
    req = drive_svc.files().get_media(fileId=target["id"])
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8")


def _upload_images_to_drive_folder(
    drive_svc: Any,
    parent_folder_id: str,
    folder_name: str,
    image_paths: list[Path],
    public_link: bool,
    retry_attempts: int = 3,
    retry_sleep_sec: float = 1.2,
) -> str:
    """
    Upload toàn bộ tệp (ảnh/zip) lên Drive vào 1 subfolder và trả link folder.

    Request gửi đi:
    1) files.create(mimeType=folder) để tạo folder.
    2) files.create(media_body=...) để upload từng tệp.
    3) permissions.create (nếu public_link=true).

    Response nhận về:
    - Link folder Drive chứa các tệp đã upload.
    """
    from googleapiclient.http import MediaFileUpload

    folder = _retry_google_call(
        lambda: drive_svc.files()
        .create(
            body={
                "name": str(folder_name or "flow_images").strip(),
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id],
            },
            fields="id,name",
        )
        .execute(),
        attempts=retry_attempts,
        sleep_sec=retry_sleep_sec,
    )
    folder_id = str(folder["id"])
    folder_link = f"https://drive.google.com/drive/folders/{folder_id}"
    _log(
        f"Tạo folder upload Drive: name='{folder_name}' | "
        f"parent='{parent_folder_id}' | folder_id={folder_id}"
    )

    if public_link:
        _retry_google_call(
            lambda: drive_svc.permissions()
            .create(fileId=folder_id, body={"type": "anyone", "role": "reader"})
            .execute(),
            attempts=retry_attempts,
            sleep_sec=retry_sleep_sec,
        )

    for img in image_paths:
        mime = mimetypes.guess_type(str(img))[0] or "application/octet-stream"
        media = MediaFileUpload(str(img), mimetype=mime, resumable=False)
        created = _retry_google_call(
            lambda: drive_svc.files()
            .create(
                body={"name": img.name, "parents": [folder_id]},
                media_body=media,
                fields="id",
            )
            .execute(),
            attempts=retry_attempts,
            sleep_sec=retry_sleep_sec,
        )
        _log(f"Upload tệp: {img.name} -> file_id={created['id']}")
        if public_link:
            _retry_google_call(
                lambda: drive_svc.permissions()
                .create(
                    fileId=str(created["id"]),
                    body={"type": "anyone", "role": "reader"},
                )
                .execute(),
                attempts=retry_attempts,
                sleep_sec=retry_sleep_sec,
            )

    return folder_link


async def _generate_scene_images_with_flow_for_prompt_file(
    prompt_file: Path,
    worker_cfg: WorkerConfig,
    scenario_name: str,
    scene_timeout_per_prompt_sec: int = 180,
    scenario_timeout_sec: int = 30 * 60,
    scene_min_success_images: int = 120,
    scene_retry_failed_rounds: int = 2,
) -> list[Path]:
    """
    Chạy luồng Flow scene-only cho 1 file prompt_image.txt.

    Ảnh output trả về dưới dạng list Path để upload Drive.
    """
    ui_cfg = load_flow_ui_settings()
    scene_mode = str(ui_cfg.get("scene_execution_mode", "serial") or "serial").strip().lower()
    max_in_flight = max(1, int(ui_cfg.get("pipeline_max_in_flight", 2) or 2))
    gap_min = float(ui_cfg.get("pipeline_send_gap_min", 1.5) or 1.5)
    gap_max = float(ui_cfg.get("pipeline_send_gap_max", 3.5) or 3.5)

    settings = FlowSettings(
        auto_apply=bool(ui_cfg.get("auto_apply", True)),
        top_mode=str(ui_cfg.get("top_mode", "image") or "image"),
        secondary_mode=str(ui_cfg.get("secondary_mode", "") or ""),
        aspect_ratio=str(ui_cfg.get("aspect_ratio", "16:9") or "16:9"),
        multiplier=str(ui_cfg.get("multiplier", "x1") or "x1"),
        model_name=str(ui_cfg.get("model_name", "Nano Banana 2") or "Nano Banana 2"),
        allow_model_alias_fallback=bool(ui_cfg.get("allow_model_alias_fallback", False)),
    )

    prompts = parse_image_prompts_file(str(prompt_file))
    if not prompts:
        raise RuntimeError(f"File prompt không có dữ liệu hợp lệ: {prompt_file}")

    scenario_dir = prompt_file.parent
    out_dir = scenario_dir / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    job = ImageJob(
        job_id=scenario_name,
        prompts=prompts,
        output_dir=str(out_dir),
        reference_images=None,
        settings=settings,
        metadata={
            "scenario_dir": str(scenario_dir),
            "scene_execution_mode": scene_mode,
            "pipeline_max_in_flight": max_in_flight,
            "pipeline_send_gap_min": gap_min,
            "pipeline_send_gap_max": gap_max,
            "pipeline_send_gap_sec": (gap_min + gap_max) / 2,
            # Đồng bộ timeout/target/retry với main_runner_no_reference.
            "scene_timeout_per_prompt_sec": int(scene_timeout_per_prompt_sec or 180),
            "scenario_timeout_sec": int(scenario_timeout_sec or (30 * 60)),
            "scene_min_success_images": int(scene_min_success_images or 120),
            "scene_retry_failed_rounds": int(scene_retry_failed_rounds or 2),
        },
    )

    async def _worker_func(page, current_job: ImageJob):
        # Bước apply panel settings được gọi ngay trước khi gửi prompt.
        if current_job.settings and current_job.settings.auto_apply:
            await apply_flow_generation_settings_panel(
                page=page,
                top_mode=current_job.settings.top_mode,
                secondary_mode=current_job.settings.secondary_mode,
                aspect_ratio=current_job.settings.aspect_ratio,
                multiplier=current_job.settings.multiplier,
                model_name=current_job.settings.model_name,
                allow_model_alias_fallback=current_job.settings.allow_model_alias_fallback,
            )
        return await generate_scene_images_from_job(page, current_job)

    pool = WorkerPool(configs=[worker_cfg])
    try:
        await pool.start_all()
        await pool.run_jobs_parallel([job], _worker_func)
    finally:
        await pool.stop_all()

    return sorted(out_dir.glob("*.png")) + sorted(out_dir.glob("*.jpg")) + sorted(out_dir.glob("*.jpeg"))


def run_sheet_drive_flow_pipeline(config: SheetFlowConfig) -> dict[str, Any]:
    """
    Hàm public duy nhất để chạy toàn bộ pipeline.

    Luồng request/response tổng quan:
    1) Request Sheets API đọc danh sách dòng cần xử lý.
       Response: nhận các dòng có link folder prompt.
    2) Request Drive API tải image_prompts.txt cho từng dòng.
       Response: prompt text theo từng cảnh.
    3) Request nội bộ Flow runner gửi prompt tạo ảnh.
       Response: ảnh local trong thư mục output.
    4) Request Drive API upload ảnh + tạo folder kết quả.
       Response: link folder ảnh Drive.
    5) Request Sheets API update cột output.
       Response: Sheet được cập nhật link theo đúng dòng.
    """
    cred_path = Path(config.credentials)
    token_path = Path(config.token_file)
    if not cred_path.exists():
        raise FileNotFoundError(f"Không tìm thấy credentials: {cred_path}")
    if not str(config.drive_output_parent_id or "").strip():
        raise ValueError("Thiếu drive_output_parent_id")
    if str(config.drive_output_parent_id).strip() in {
        "ID_FOLDER_DRIVE_THAT",
        "DRIVE_PARENT_FOLDER_ID_CAN_GHI_ANH",
    }:
        raise ValueError(
            "drive_output_parent_id đang là placeholder. Hãy thay bằng folder ID thật trên Drive."
        )

    sheets_svc, drive_svc = _build_google_services(cred_path, token_path)
    _log("Auth Google thành công (Sheets + Drive).")

    sheet_id = _extract_sheet_id(config.sheet)
    meta = _retry_google_call(
        lambda: sheets_svc.spreadsheets().get(spreadsheetId=sheet_id).execute(),
        attempts=config.google_retry_attempts,
        sleep_sec=config.google_retry_sleep_sec,
    )
    tab_name = meta["sheets"][0]["properties"]["title"]
    sheet_title = meta.get("properties", {}).get("title", sheet_id)
    _log(f"Đọc Sheet: '{sheet_title}' | tab='{tab_name}' | sheet_id={sheet_id}")

    rows, hidden_links = _read_sheet_rows_with_hidden_links(
        sheets_svc=sheets_svc,
        spreadsheet_id=sheet_id,
        tab_name=tab_name,
        columns=config.range_columns,
        max_rows=config.max_rows,
        retry_attempts=config.google_retry_attempts,
        retry_sleep_sec=config.google_retry_sleep_sec,
    )
    if not rows:
        raise RuntimeError("Sheet không có dữ liệu.")
    _log(f"Tải dữ liệu sheet: rows={len(rows)} | hidden_links={len(hidden_links)}")

    header_idx = _find_header_row(rows, config.col_prompt_folder)
    header = rows[header_idx]
    col_prompt = _find_col(header, config.col_prompt_folder)
    col_title = _find_col(header, config.col_title)
    col_output = _find_col(header, config.col_output_folder)
    if col_prompt is None:
        raise RuntimeError(f"Không tìm thấy cột '{config.col_prompt_folder}'.")
    if col_output is None:
        raise RuntimeError(f"Không tìm thấy cột '{config.col_output_folder}'.")
    _log(
        f"Map cột: prompt='{config.col_prompt_folder}'(idx={col_prompt}) | "
        f"title='{config.col_title}'(idx={col_title}) | "
        f"output='{config.col_output_folder}'(idx={col_output})"
    )

    worker_rows = json.loads(Path(config.video_workers_config).read_text(encoding="utf-8"))
    workers = worker_rows.get("video_workers") or worker_rows.get("workers") or []
    if not workers:
        raise RuntimeError("config/video_workers.json không có worker.")

    first_worker = workers[0]
    worker_cfg = WorkerConfig(
        worker_id=str(first_worker.get("worker_id", "video_1")),
        profile_dir=str(first_worker.get("profile_dir", "")),
        proxy=(str(first_worker.get("proxy")) if config.use_proxy and first_worker.get("proxy") else None),
    )
    if not worker_cfg.profile_dir:
        raise RuntimeError("Worker profile_dir đang rỗng.")
    _log(
        f"Dùng worker: {worker_cfg.worker_id} | profile={worker_cfg.profile_dir} | "
        f"use_proxy={config.use_proxy}"
    )

    base_workspace = Path(config.workspace_dir)
    base_workspace.mkdir(parents=True, exist_ok=True)

    result_items: list[dict[str, Any]] = []
    ok = 0
    fail = 0

    for r_idx in range(header_idx + 1, len(rows)):
        row = rows[r_idx]
        row_number = r_idx + 1
        if row_number < max(1, int(config.row_start or 1)):
            continue
        if int(config.row_end or 0) > 0 and row_number > int(config.row_end):
            break

        prompt_text = row[col_prompt].strip() if col_prompt < len(row) else ""
        prompt_hidden = hidden_links.get((r_idx, col_prompt), "").strip()
        prompt_folder_link = prompt_hidden or prompt_text
        existing_output = row[col_output].strip() if col_output < len(row) else ""
        title_text = row[col_title].strip() if (col_title is not None and col_title < len(row)) else ""

        if not prompt_folder_link:
            _log(f"Row {row_number}: bỏ qua vì 'Prompt tạo ảnh' trống.")
            result_items.append(
                {"row": row_number, "status": "skip", "reason": "empty_prompt_folder"}
            )
            continue
        if not _is_drive_link_or_id(prompt_folder_link):
            _log(
                f"Row {row_number}: bỏ qua vì 'Prompt tạo ảnh' không phải link/ID Drive hợp lệ: "
                f"'{prompt_folder_link[:80]}'"
            )
            result_items.append(
                {"row": row_number, "status": "skip", "reason": "invalid_prompt_folder_link"}
            )
            continue
        if existing_output:
            _log(f"Row {row_number}: bỏ qua vì đã có link output.")
            result_items.append(
                {"row": row_number, "status": "skip", "reason": "already_has_output"}
            )
            continue

        safe_title = _sanitize_name(title_text, fallback=f"row_{row_number}")
        scenario_name = f"{sheet_id}_r{row_number}_{safe_title}"
        scenario_dir = base_workspace / scenario_name
        scenario_dir.mkdir(parents=True, exist_ok=True)
        prompt_file = scenario_dir / SCENARIO_IMAGE_FILE

        try:
            _log(
                f"Row {row_number}: bắt đầu xử lý | title='{title_text[:80]}' | "
                f"prompt_folder='{prompt_folder_link[:120]}'"
            )
            prompt_content = _download_text_file_from_drive_folder(
                drive_svc=drive_svc,
                folder_link_or_id=prompt_folder_link,
                target_filename=config.drive_prompt_filename,
                retry_attempts=config.google_retry_attempts,
                retry_sleep_sec=config.google_retry_sleep_sec,
            )
            prompt_file.write_text(prompt_content, encoding="utf-8")
            _log(f"Row {row_number}: đã lưu prompt local -> {prompt_file}")

            # Luôn dọn ảnh output cũ của row hiện tại để đảm bảo upload đúng batch mới nhất.
            output_dir = scenario_dir / "output"
            deleted_old = _cleanup_local_output_images(output_dir)
            if deleted_old > 0:
                _log(f"Row {row_number}: đã dọn {deleted_old} ảnh cũ trong output trước khi generate.")

            local_images = asyncio.run(
                _generate_scene_images_with_flow_for_prompt_file(
                    prompt_file=prompt_file,
                    worker_cfg=worker_cfg,
                    scenario_name=scenario_name,
                    scene_timeout_per_prompt_sec=config.scene_timeout_per_prompt_sec,
                    scenario_timeout_sec=config.scenario_timeout_sec,
                    scene_min_success_images=config.scene_min_success_images,
                    scene_retry_failed_rounds=config.scene_retry_failed_rounds,
                )
            )
            if not local_images:
                raise RuntimeError("Flow không tạo được ảnh nào.")
            _log(f"Row {row_number}: số ảnh local sẵn sàng upload = {len(local_images)}")

            # Nén ảnh thành 1 file zip để giảm số request upload và tăng tốc đáng kể.
            zip_path = _build_zip_from_images(
                image_paths=local_images,
                zip_path=scenario_dir / f"{scenario_name}.zip",
            )
            _log(f"Row {row_number}: đã nén ảnh -> {zip_path.name}")

            drive_folder_link = _upload_images_to_drive_folder(
                drive_svc=drive_svc,
                parent_folder_id=str(config.drive_output_parent_id).strip(),
                folder_name=scenario_name,
                image_paths=[zip_path],
                public_link=bool(config.public_link),
                retry_attempts=config.google_retry_attempts,
                retry_sleep_sec=config.google_retry_sleep_sec,
            )

            cell = f"'{tab_name}'!{_col_index_to_a1(col_output)}{row_number}"
            _retry_google_call(
                lambda: sheets_svc.spreadsheets()
                .values()
                .update(
                    spreadsheetId=sheet_id,
                    range=cell,
                    valueInputOption="RAW",
                    body={"values": [[drive_folder_link]]},
                )
                .execute(),
                attempts=config.google_retry_attempts,
                sleep_sec=config.google_retry_sleep_sec,
            )
            _log(f"Row {row_number}: ghi link về sheet thành công -> {cell}")

            ok += 1
            result_items.append(
                {
                    "row": row_number,
                    "status": "ok",
                    "images": len(local_images),
                    "zip_file": str(zip_path),
                    "drive_folder": drive_folder_link,
                    "scenario_dir": str(scenario_dir),
                }
            )
        except Exception as exc:
            fail += 1
            _log(f"Row {row_number}: lỗi -> {exc}")
            result_items.append(
                {"row": row_number, "status": "error", "error": str(exc)}
            )

    return {
        "sheet_id": sheet_id,
        "sheet_title": sheet_title,
        "ok": ok,
        "fail": fail,
        "items": result_items,
    }
