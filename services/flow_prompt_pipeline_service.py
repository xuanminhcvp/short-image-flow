from __future__ import annotations

import asyncio
import json
import random  # Dùng để tạo khoảng nghỉ ngẫu nhiên giữa các prompt
import re
import time
from collections import defaultdict
from typing import Any

from models.image_job import ImageJob
from services.flow_humanize_service import (
    handle_unusual_activity_with_cooldown,
    sleep_humanized,
)
from services.flow_image_service import (
    download_flow_images,
    run_flow_image_capture,
)
from services.flow_prompt_service import (
    click_flow_generate_button,
    find_and_focus_flow_prompt,
    send_flow_prompt,
    type_flow_prompt,
)
from services.prompt_media_map_service import (
    build_prompt_media_batch,
    collect_media_ids_from_obj,
    normalize_media_url,
)


def _looks_like_api_url(url: str) -> bool:
    """
    Check nhanh URL có phải API liên quan đến generate image không.
    """
    u = str(url or "").lower()
    if not u:
        return False
    return (
        "flowmedia:batchgenerateimages" in u
        or "media.getmediaurlredirect" in u
        or "/api/trpc/" in u
    )


def _extract_prompt_index_from_post_data(post_data: str, prompts: list[str]) -> int:
    """
    Map request body -> prompt_index bằng cách tìm prompt text trong post_data.
    """
    body = str(post_data or "")
    if not body:
        return 0
    for idx, prompt_text in enumerate(prompts, start=1):
        p = str(prompt_text or "").strip()
        if p and p in body:
            return idx
    return 0


def _collect_ids_and_urls(obj: Any, out_urls: set[str]) -> None:
    """
    Duyệt JSON response để gom các URL ảnh.
    """
    if isinstance(obj, dict):
        for _k, v in obj.items():
            if isinstance(v, str):
                low = v.lower()
                if low.startswith("http://") or low.startswith("https://"):
                    out_urls.add(v)
            _collect_ids_and_urls(v, out_urls)
    elif isinstance(obj, list):
        for x in obj:
            _collect_ids_and_urls(x, out_urls)


def _media_id_to_redirect_url(media_id: str) -> str:
    """
    Chuẩn hóa media_id thành URL redirect để tải ảnh qua session hiện tại.
    """
    mid = str(media_id or "").strip()
    if not mid:
        return ""
    return f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?name={mid}"


async def generate_scene_images_with_pipeline(
    page,
    job: ImageJob,
    timeout_per_prompt_sec: int = 180,
    expected_images_per_prompt: int = 1,
    max_in_flight: int = 2,
    send_gap_sec: float = 1.8,  # Giữ tham số này để tương thích ngược với code cũ
    min_success_images: int = 120,
    scenario_timeout_sec: int = 30 * 60,
    retry_failed_rounds: int = 1,
) -> list[str]:
    """
    Chạy scene prompts theo kiểu pipeline trong 1 tab:
    - Không đợi prompt trước tải xong mới gửi prompt sau.
    - Giữ số prompt "đang bay" theo max_in_flight.
    - Map prompt -> ảnh dựa trên API/media_id.
    - Khoảng nghỉ giữa 2 lần gửi là ngẫu nhiên (lấy từ metadata job nếu có).

    Request gửi đi:
    1) Gửi prompt từng cái với nhịp ngẫu nhiên trong [send_gap_min, send_gap_max].
    2) Song song nghe API response để biết prompt nào đã có media_id.
    3) Prompt nào đạt expected ảnh hoặc timeout sẽ "hoàn tất", nhả slot cho prompt tiếp.

    Response nhận về:
    - Danh sách src ảnh (URL redirect) đã map theo từng prompt.
    - Ảnh tải về lưu theo prefix giống mode tuần tự: <job_id>_001_img1.png, ...
    """
    results: list[str] = []

    prompts = list(job.prompts or [])
    if not prompts:
        print(f"[WARN] Job {job.job_id} không có prompt scene nào để chạy.")
        return results

    expected = max(1, int(expected_images_per_prompt or 1))
    max_flight = max(1, int(max_in_flight or 1))
    timeout_one = max(20, int(timeout_per_prompt_sec or 180))
    stage_timeout = max(120, int(scenario_timeout_sec or 30 * 60))
    min_required = max(1, min(int(min_success_images or 120), len(prompts)))
    retry_rounds = max(0, int(retry_failed_rounds or 0))

    # — Đọc khoảng nghỉ ngẫu nhiên từ metadata job (nếu có) —
    # Ưu tiên pipeline_send_gap_min/max (format mới) óị tương thích ngược pipeline_send_gap_sec.
    meta = job.metadata or {}
    gap_min = float(meta.get("pipeline_send_gap_min") or 0) or max(0.5, send_gap_sec * 0.8)
    gap_max = float(meta.get("pipeline_send_gap_max") or 0) or max(gap_min, send_gap_sec * 1.3)
    # Clamp để tránh giá trị vô lý
    gap_min = max(0.5, gap_min)
    gap_max = max(gap_min, gap_max)

    print(
        f"[*] [pipeline] mode=pipeline | prompts={len(prompts)} | "
        f"max_in_flight={max_flight} | expected={expected} | "
        f"send_gap=[{gap_min:.1f} - {gap_max:.1f}]s"
    )
    print(
        f"[*] [pipeline] timeout_per_prompt={timeout_one}s | "
        f"scenario_timeout={stage_timeout}s | min_success={min_required} | "
        f"retry_rounds={retry_rounds}"
    )

    # --- State tracking ---
    request_meta_by_id: dict[int, dict[str, Any]] = {}
    prompt_to_media_ids: dict[int, set[str]] = defaultdict(set)
    prompt_to_api_urls: dict[int, set[str]] = defaultdict(set)
    prompt_send_ts: dict[int, float] = {}
    prompt_done_reason: dict[int, str] = {}
    network_events: list[dict[str, Any]] = []
    response_tasks: set[asyncio.Task] = set()

    def _on_task_done(t: asyncio.Task) -> None:
        response_tasks.discard(t)

    async def _handle_api_response(response) -> None:
        """
        Parse response API để gom media_id cho từng prompt.
        """
        try:
            req = response.request
            req_id = id(req)
            meta = request_meta_by_id.get(req_id, {})
            prompt_index = int(meta.get("prompt_index", 0) or 0)

            status = int(response.status or 0)
            ct = str((response.headers or {}).get("content-type", "") or "").lower()
            if status >= 500:
                return

            body_json = None
            if "json" in ct or "text" in ct:
                raw = await response.text()
                try:
                    body_json = json.loads(raw)
                except Exception:
                    body_json = None

            if body_json is None:
                return

            media_ids: set[str] = set()
            api_urls: set[str] = set()
            collect_media_ids_from_obj(body_json, media_ids)
            _collect_ids_and_urls(body_json, api_urls)

            if prompt_index > 0:
                for mid in media_ids:
                    prompt_to_media_ids[prompt_index].add(str(mid))
                for u in api_urls:
                    prompt_to_api_urls[prompt_index].add(normalize_media_url(u))

            network_events.append(
                {
                    "type": "api_response",
                    "prompt_index": prompt_index,
                    "status": status,
                    "media_ids_count": len(media_ids),
                    "urls_count": len(api_urls),
                }
            )
        except Exception as e:
            network_events.append({"type": "api_response_parse_error", "error": str(e)})

    def _on_request(req) -> None:
        try:
            if req.resource_type not in {"fetch", "xhr"}:
                return
            url = str(req.url or "")
            if not _looks_like_api_url(url):
                return

            post_data = ""
            try:
                post_data = str(req.post_data or "")
            except Exception:
                post_data = ""

            prompt_index = _extract_prompt_index_from_post_data(post_data, prompts)
            request_meta_by_id[id(req)] = {
                "url": url,
                "method": str(req.method or ""),
                "prompt_index": prompt_index,
            }
            network_events.append(
                {
                    "type": "api_request",
                    "url": url,
                    "method": str(req.method or ""),
                    "prompt_index": prompt_index,
                }
            )
        except Exception:
            pass

    def _on_response(resp) -> None:
        try:
            req = resp.request
            if req.resource_type in {"fetch", "xhr"} and _looks_like_api_url(req.url or ""):
                t = asyncio.create_task(_handle_api_response(resp))
                response_tasks.add(t)
                t.add_done_callback(_on_task_done)
        except Exception:
            pass

    # Đăng ký listeners trước khi gửi prompt để không hụt event đầu tiên.
    page.on("request", _on_request)
    page.on("response", _on_response)

    sent_count = 0
    done_prompts: set[int] = set()
    success_prompt_indices: set[int] = set()
    total = len(prompts)
    global_start = time.monotonic()
    global_deadline = global_start + stage_timeout

    try:
        while len(done_prompts) < total and time.monotonic() < global_deadline:
            now = time.monotonic()

            # 1) Update trạng thái prompt hoàn tất.
            for pidx in range(1, sent_count + 1):
                if pidx in done_prompts:
                    continue
                media_count = len(prompt_to_media_ids.get(pidx, set()))
                age = now - float(prompt_send_ts.get(pidx, now))
                if media_count >= expected:
                    done_prompts.add(pidx)
                    prompt_done_reason[pidx] = f"enough_media_ids({media_count})"
                    print(f"[*] [pipeline] prompt {pidx:02d} hoàn tất: đủ ảnh ({media_count}).")
                elif age >= timeout_one:
                    done_prompts.add(pidx)
                    prompt_done_reason[pidx] = f"timeout({int(age)}s)"
                    print(
                        f"[WARN] [pipeline] prompt {pidx:02d} timeout {int(age)}s, "
                        f"media_ids={media_count}. Nhả slot để chạy tiếp."
                    )

            # 2) Gửi prompt mới nếu còn slot trống.
            in_flight = sent_count - len(done_prompts)
            while sent_count < total and in_flight < max_flight:
                if time.monotonic() >= global_deadline:
                    break
                idx = sent_count + 1
                prompt_text = prompts[idx - 1]
                await sleep_humanized(0.5, floor=0.2)
                print(f"[*] [pipeline] Gửi prompt {idx}/{total}...")

                await find_and_focus_flow_prompt(page)
                await type_flow_prompt(page, prompt_text)
                sent_ok = await send_flow_prompt(page)
                if not sent_ok:
                    await click_flow_generate_button(page)

                await handle_unusual_activity_with_cooldown(
                    page,
                    stage_label=f"scene_pipeline_prompt_{idx}",
                )

                prompt_send_ts[idx] = time.monotonic()
                sent_count += 1
                in_flight += 1
                if sent_count < total:
                    # Khoảng nghỉ ngẫu nhiên giữa 2 lần gửi prompt liên tiếp.
                    # Giúp tránh nhịp cố định dễ bị hệ thống phát hiện là bot.
                    actual_gap = random.uniform(gap_min, gap_max)
                    print(f"[*] [pipeline] Nghỉ {actual_gap:.2f}s trước prompt tiếp theo...")
                    await asyncio.sleep(actual_gap)

            # 3) Nhịp poll ngắn để lấy response mới.
            await asyncio.sleep(1.0)

        # Chờ thêm vài giây để flush response đang bay cuối phiên.
        flush_until = time.monotonic() + 6.0
        while time.monotonic() < flush_until and response_tasks:
            await asyncio.sleep(0.25)

    finally:
        # Gỡ listeners để tránh ảnh hưởng stage khác.
        try:
            page.remove_listener("request", _on_request)
        except Exception:
            pass
        try:
            page.remove_listener("response", _on_response)
        except Exception:
            pass

    # --- Download theo prompt map ---
    print("[*] [pipeline] Bắt đầu tải ảnh theo map API/media_id...")
    for idx, prompt_text in enumerate(prompts, start=1):
        if time.monotonic() >= global_deadline:
            print("[WARN] [pipeline] Hết thời gian kịch bản, dừng bước tải map.")
            break
        media_ids = sorted(prompt_to_media_ids.get(idx, set()))
        mapped_srcs: list[str] = []
        seen_srcs: set[str] = set()

        # Ưu tiên nguồn chắc chắn nhất: media_id từ API response.
        for mid in media_ids:
            u = _media_id_to_redirect_url(mid)
            if u and u not in seen_srcs:
                seen_srcs.add(u)
                mapped_srcs.append(u)

        # Bổ sung URL parse được từ API để tăng tỉ lệ tải thành công.
        # Trường hợp redirect URL theo media_id bị trả lỗi tạm thời thì vẫn còn phương án khác.
        for u in sorted(prompt_to_api_urls.get(idx, set())):
            nu = normalize_media_url(u)
            if nu and nu not in seen_srcs:
                seen_srcs.add(nu)
                mapped_srcs.append(nu)

        # Giới hạn số URL thử cho mỗi prompt để không kéo dài quá lâu.
        # expected thường = 1, ở đây cho thử tối đa 4 URL để tăng độ bền tải.
        max_try_urls = max(1, min(4, len(mapped_srcs)))
        mapped_srcs = mapped_srcs[:max_try_urls]

        prefix = f"{job.job_id}_{idx:03d}"
        saved = 0
        selected_srcs: list[str] = []
        if mapped_srcs:
            # Thử lần lượt từng URL ứng viên, chỉ cần save được 1 ảnh là coi prompt này pass.
            # Cách này xử lý tốt case có media_id nhưng URL đầu tiên lỗi/expire.
            for src_try in mapped_srcs:
                saved = await download_flow_images(
                    page=page,
                    new_srcs=[src_try],
                    output_dir=job.output_dir,
                    prefix=prefix,
                )
                if saved > 0:
                    selected_srcs = [src_try]
                    break

        capture_result = {
            "ok": saved > 0,
            "saved": int(saved),
            "new_srcs": (selected_srcs if saved > 0 else list(mapped_srcs)),
            "baseline_count": 0,
            "download_limit": None,
        }
        batch = build_prompt_media_batch(
            mode="scene_pipeline",
            prompt_index=idx,
            prompt_total=total,
            prompt_text=prompt_text,
            output_dir=job.output_dir,
            prefix=prefix,
            expected_count=expected,
            capture_result=capture_result,
        )
        print(
            f"[*] [pipeline-map] prompt {idx:02d}/{total} | "
            f"media_ids={len(media_ids)} srcs={len(batch.new_srcs)} files={len(batch.generated_files)} "
            f"| done_reason={prompt_done_reason.get(idx, 'n/a')}"
        )
        if batch.ok:
            results.extend(batch.new_srcs)
            success_prompt_indices.add(idx)
        else:
            print(f"[ERR] [pipeline] Prompt {idx:02d} không tải được ảnh nào.")

    # --- Retry các prompt fail để đẩy tỷ lệ thành công ---
    if retry_rounds > 0 and len(success_prompt_indices) < min_required:
        for retry_no in range(1, retry_rounds + 1):
            if len(success_prompt_indices) >= min_required:
                break
            if time.monotonic() >= global_deadline:
                break
            failed_indices = [i for i in range(1, total + 1) if i not in success_prompt_indices]
            if not failed_indices:
                break
            print(
                f"[*] [pipeline-retry] Round {retry_no}/{retry_rounds} | "
                f"failed_prompts={len(failed_indices)} | success={len(success_prompt_indices)}/{total}"
            )
            for idx in failed_indices:
                if len(success_prompt_indices) >= min_required:
                    break
                now = time.monotonic()
                if now >= global_deadline:
                    break

                remain_sec = int(global_deadline - now)
                timeout_this = max(30, min(timeout_one, remain_sec - 2))
                if timeout_this <= 0:
                    break

                prompt_text = prompts[idx - 1]
                print(
                    f"[*] [pipeline-retry] prompt {idx:02d}/{total} | "
                    f"timeout={timeout_this}s | remain={remain_sec}s"
                )

                await find_and_focus_flow_prompt(page)
                await type_flow_prompt(page, prompt_text)
                sent_ok = await send_flow_prompt(page)
                if not sent_ok:
                    await click_flow_generate_button(page)
                await handle_unusual_activity_with_cooldown(
                    page,
                    stage_label=f"scene_pipeline_retry_prompt_{idx}",
                )

                retry_prefix = f"{job.job_id}_{idx:03d}_retry{retry_no}"
                capture_result = await run_flow_image_capture(
                    page=page,
                    output_dir=job.output_dir,
                    prefix=retry_prefix,
                    expected=expected,
                    timeout=timeout_this,
                )
                batch = build_prompt_media_batch(
                    mode="scene_pipeline_retry",
                    prompt_index=idx,
                    prompt_total=total,
                    prompt_text=prompt_text,
                    output_dir=job.output_dir,
                    prefix=retry_prefix,
                    expected_count=expected,
                    capture_result=capture_result,
                )
                if batch.ok:
                    success_prompt_indices.add(idx)
                    results.extend(batch.new_srcs)
                    print(
                        f"[OK] [pipeline-retry] Prompt {idx:02d} hồi phục thành công "
                        f"(saved={batch.saved_count})."
                    )
                else:
                    print(f"[WARN] [pipeline-retry] Prompt {idx:02d} vẫn chưa có ảnh sau retry.")

    print(
        f"[*] [pipeline-debug] network_events={len(network_events)} | "
        f"sent={sent_count}/{total} | done={len(done_prompts)}/{total} | "
        f"success={len(success_prompt_indices)}/{total}"
    )

    if len(success_prompt_indices) < min_required:
        # Mục tiêu mềm: cố gắng đạt min_required trước deadline.
        # Nếu hết 30 phút mà chưa đạt thì vẫn dùng số ảnh hiện có, không fail kịch bản.
        print(
            f"[WARN] [pipeline] Hết thời gian hoặc hết retry: "
            f"success={len(success_prompt_indices)}/{total}, target={min_required}. "
            "Tiếp tục dùng ảnh đã có."
        )
    return results
