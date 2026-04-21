import asyncio
import os
import json
import shutil
from pathlib import Path
import argparse

from models.worker_config import WorkerConfig
from models.image_job import ImageJob
from models.flow_settings import FlowSettings
from services.worker_pool_service import WorkerPool
from services.flow_generate_service import generate_images_from_job
from services.prompt_service import parse_character_file, parse_image_prompts_file, SCENARIO_CHARACTER_FILE, SCENARIO_IMAGE_FILE

def _parse_args():
    """
    Parse tham số dòng lệnh.
    - --no-proxy: chạy bằng mạng thật, không gán proxy vào worker.
    - --scenario: chỉ chạy kịch bản chỉ định.
      Hỗ trợ nhập:
      + A  -> tự đổi thành kich_ban_A
      + kich_ban_A -> dùng nguyên văn
    """
    parser = argparse.ArgumentParser(description="Runner đơn giản cho luồng tạo ảnh theo kịch bản.")
    parser.add_argument(
        "--no-proxy",
        action="store_true",
        help="Không dùng proxy, chạy mạng thật."
    )
    parser.add_argument(
        "--scenario",
        type=str,
        help="Chỉ chạy một kịch bản. Ví dụ: A hoặc kich_ban_A"
    )
    return parser.parse_args()


def _normalize_scenario_name(raw_name: str | None) -> str | None:
    """
    Chuẩn hóa tên kịch bản người dùng nhập thành đúng tên folder.
    """
    if not raw_name:
        return None
    name = str(raw_name).strip()
    if not name:
        return None
    if name.startswith("kich_ban_"):
        return name
    return f"kich_ban_{name}"


async def main(args):
    # 1. Lọc kịch bản
    scenarios_dir = "scenarios"
    valid_folders = [f for f in os.listdir(scenarios_dir) 
                    if os.path.isdir(os.path.join(scenarios_dir, f)) and f.startswith("kich_ban_")]

    # Nếu có chỉ định --scenario thì chỉ giữ lại đúng kịch bản đó.
    selected_scenario = _normalize_scenario_name(args.scenario)
    if selected_scenario:
        valid_folders = [f for f in valid_folders if f == selected_scenario]
        if not valid_folders:
            print(f"Không tìm thấy kịch bản '{selected_scenario}' trong thư mục scenarios/.")
            return
        print(f"[i] Chỉ chạy kịch bản: {selected_scenario}")
    
    if not valid_folders:
        print("Không tìm thấy kịch bản nào trong thư mục scenarios/.")
        return

    # 2. Xây dựng danh sách ImageJobs
    jobs = []
    # Setting (Dùng tạm mặc định 16:9, sau này bạn update có thể dùng hàm đọc file UI)
    default_settings = FlowSettings(aspect_ratio="16:9", model_name="Nano Banana Pro")

    for folder in valid_folders:
        folder_path = os.path.join(scenarios_dir, folder)
        char_file = os.path.join(folder_path, SCENARIO_CHARACTER_FILE)
        img_file = os.path.join(folder_path, SCENARIO_IMAGE_FILE)

        if not os.path.exists(img_file):
            print(f"Bỏ qua {folder}: Thiếu file prompt_image.txt")
            continue

        # Load prompts
        img_prompts = parse_image_prompts_file(img_file)
        
        # Load reference images (nếu kịch bản có ref)
        reference_list = []
        char_data = {}
        if os.path.exists(char_file):
            char_data = parse_character_file(char_file)
            # Ở phiên bản hoàn thiện, file tạo ra sẽ là dạng: folder_path/char_XXX.png
            # Hiện tại cứ truyền một list dummy để đánh dấu là "CÓ REF" để engine nhận biết
            for char_id in char_data.keys():
                # Đường dẫn ảo tượng trưng, engine sẽ biết có nhân vật
                reference_list.append(os.path.join(folder_path, f"{char_id}.png"))

        job = ImageJob(
            job_id=folder,
            prompts=img_prompts,
            reference_images=reference_list if reference_list else None,
            output_dir=os.path.join(folder_path, "output"),
            settings=default_settings,
            # Lưu metadata để stage reference có đủ dữ liệu tạo ảnh nhân vật khi thiếu file.
            metadata={
                "scenario_dir": folder_path,
                "character_prompts": char_data,
            },
        )
        jobs.append(job)

    if not jobs:
        print("Không có Job nào hợp lệ.")
        return

    # 3. Đọc Config Workers
    with open("config/video_workers.json", "r") as f:
        cfg_data = json.load(f)

    # Ưu tiên key "video_workers" (config hiện tại), fallback "workers" (config cũ).
    # Mục tiêu: không bắt người dùng phải chỉnh lại file config khi đổi phiên bản.
    worker_rows = cfg_data.get("video_workers") or cfg_data.get("workers") or []
    if not worker_rows:
        print("Không tìm thấy worker nào trong config/video_workers.json.")
        return

    # Nếu chỉ chạy 1 kịch bản, ưu tiên worker có scenario_dir khớp.
    # Ví dụ: --scenario A -> worker có scenario_dir = scenarios/kich_ban_A
    if selected_scenario:
        matched_workers = []
        for w in worker_rows:
            scenario_dir = str(w.get("scenario_dir", "") or "")
            scenario_name = os.path.basename(scenario_dir)
            if scenario_name == selected_scenario:
                matched_workers.append(w)
        if matched_workers:
            worker_rows = matched_workers
            print(f"[i] Tự chọn worker theo scenario: {matched_workers[0].get('worker_id', 'unknown')}")

    workers = []
    # Cắt số lượng Worker vừa đủ với số lượng kịch bản (nếu số kịch bản ít hơn số worker)
    max_workers_needed = min(len(jobs), len(worker_rows))

    use_proxy = not args.no_proxy
    if not use_proxy:
        print("[!] Chế độ KHÔNG dùng Proxy đã được bật (--no-proxy). Đang dùng mạng thật.")

    for w in worker_rows[:max_workers_needed]:
        workers.append(WorkerConfig(
            worker_id=w["worker_id"],
            profile_dir=w["profile_dir"],
            proxy=w.get("proxy") if use_proxy else None
        ))

    if not workers:
        print("Không tạo được worker nào để chạy job.")
        return

    # 4. Bật Pool và uỷ quyền thực thi
    pool = WorkerPool(configs=workers)
    try:
        await pool.start_all()
        await pool.run_jobs_parallel(jobs, generate_images_from_job)
    finally:
        await pool.stop_all()
        print("Tất cả đã hoàn tất!")

if __name__ == "__main__":
    cli_args = _parse_args()
    asyncio.run(main(cli_args))
