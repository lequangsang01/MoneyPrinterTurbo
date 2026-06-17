import json
import os
import threading
import time
from datetime import datetime
from loguru import logger

from app.config import config
from app.models import const
from app.models.schema import VideoParams
from app.services import task as tm
from app.services import state as sm
from app.utils import utils

# Các hằng số trạng thái hàng chờ
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"

class QueueManager:
    """
    Lớp quản lý hàng chờ tạo video chạy tuần tự dưới nền (background worker).
    Đảm bảo an toàn đa luồng (thread-safe) và lưu trạng thái xuống ổ đĩa.
    """
    def __init__(self):
        self.lock = threading.RLock()
        self.queue_file = os.path.join(utils.storage_dir(), "tasks", "queue.json")
        self.tasks = []
        self.worker_thread = None
        self.running = False
        self._load_queue()
        self.start_worker()

    def _load_queue(self):
        """Đọc danh sách hàng chờ từ file JSON."""
        with self.lock:
            if os.path.exists(self.queue_file):
                try:
                    with open(self.queue_file, "r", encoding="utf-8") as f:
                        self.tasks = json.load(f)
                        # Nếu server bị crash/tắt khi đang chạy, chuyển trạng thái về failed
                        for t in self.tasks:
                            if t.get("status") == STATUS_PROCESSING:
                                t["status"] = STATUS_FAILED
                                t["error"] = "Bị gián đoạn do khởi động lại hệ thống"
                                sm.state.update_task(t["task_id"], state=const.TASK_STATE_FAILED)
                except Exception as e:
                    logger.error(f"Không thể đọc file hàng chờ: {e}")
                    self.tasks = []
            else:
                self.tasks = []

    def _save_queue(self):
        """Ghi danh sách hàng chờ vào file JSON."""
        with self.lock:
            try:
                task_dir = os.path.dirname(self.queue_file)
                if not os.path.exists(task_dir):
                    os.makedirs(task_dir)
                with open(self.queue_file, "w", encoding="utf-8") as f:
                    json.dump(self.tasks, f, ensure_ascii=False, indent=4)
            except Exception as e:
                logger.error(f"Không thể ghi file hàng chờ: {e}")

    def add_task(self, subject: str, params: VideoParams) -> str:
        """Thêm tác vụ mới vào hàng chờ."""
        task_id = utils.get_uuid()
        task_dir = utils.task_dir(task_id) # Tạo thư mục cho task
        
        # Serialize VideoParams sang dict
        serialized_params = params.model_dump()
        
        task_entry = {
            "task_id": task_id,
            "subject": subject or "Chủ đề tự động",
            "params": serialized_params,
            "status": STATUS_PENDING,
            "progress": 0,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": "",
            "error": "",
            "videos": []
        }
        
        with self.lock:
            self.tasks.insert(0, task_entry) # Đưa lên đầu danh sách hiển thị
            self._save_queue()
            # Cập nhật state toàn cục (MemoryState/RedisState)
            sm.state.update_task(task_id, state=const.TASK_STATE_PROCESSING, progress=0)
            
        logger.info(f"Đã thêm tác vụ {task_id} ({subject}) vào hàng chờ.")
        return task_id

    def delete_task(self, task_id: str):
        """Xóa tác vụ khỏi danh sách và xóa thư mục vật lý."""
        with self.lock:
            self.tasks = [t for t in self.tasks if t["task_id"] != task_id]
            self._save_queue()
            sm.state.delete_task(task_id)
            
        # Xóa thư mục chứa tài nguyên task
        tasks_dir = utils.task_dir()
        current_task_dir = os.path.join(tasks_dir, task_id)
        if os.path.exists(current_task_dir):
            import shutil
            try:
                shutil.rmtree(current_task_dir)
            except Exception as e:
                logger.error(f"Lỗi khi xóa thư mục task {task_id}: {e}")

    def get_tasks(self):
        """Lấy danh sách các tác vụ trong hàng chờ."""
        with self.lock:
            # Đồng bộ tiến độ từ state toàn cục của tác vụ đang chạy
            for t in self.tasks:
                if t["status"] == STATUS_PROCESSING:
                    task_state = sm.state.get_task(t["task_id"])
                    if task_state:
                        t["progress"] = task_state.get("progress", 0)
            return list(self.tasks)

    def start_worker(self):
        """Khởi chạy luồng xử lý hàng chờ dưới nền."""
        with self.lock:
            if not self.running:
                self.running = True
                self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
                self.worker_thread.start()
                logger.info("Background Worker xử lý hàng chờ đã khởi động.")

    def _worker_loop(self):
        """Vòng lặp tìm kiếm và xử lý tác vụ tuần tự."""
        while self.running:
            task_to_run = None
            with self.lock:
                # Tìm task ở trạng thái pending (ưu tiên task cũ nhất đã thêm vào - ở cuối mảng của ta)
                for t in reversed(self.tasks):
                    if t["status"] == STATUS_PENDING:
                        task_to_run = t
                        break
            
            if task_to_run:
                self._run_task_sequence(task_to_run)
            else:
                time.sleep(2)

    def _run_task_sequence(self, task):
        """Thực thi một tác vụ sinh video."""
        task_id = task["task_id"]
        logger.info(f"Bắt đầu xử lý tác vụ tuần tự: {task_id}")
        
        with self.lock:
            task["status"] = STATUS_PROCESSING
            task["progress"] = 5
            self._save_queue()
            
        try:
            # Khởi tạo VideoParams từ dict đã lưu
            params = VideoParams(**task["params"])
            
            # Chạy tiến trình sinh video đồng bộ trong luồng này
            result = tm.start(task_id=task_id, params=params)
            
            with self.lock:
                if result and "videos" in result:
                    task["status"] = STATUS_COMPLETE
                    task["progress"] = 100
                    # Lấy đường dẫn video tương đối/tuyệt đối
                    task["videos"] = result.get("videos", [])
                else:
                    task["status"] = STATUS_FAILED
                    task["error"] = "Sinh video thất bại không rõ nguyên nhân."
                task["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._save_queue()
                
        except Exception as e:
            logger.error(f"Lỗi khi thực thi task {task_id}: {e}", exc_info=True)
            with self.lock:
                task["status"] = STATUS_FAILED
                task["error"] = str(e)
                task["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._save_queue()
                sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)

# Khởi tạo một thực thể duy nhất dùng chung cho giao diện WebUI
queue_manager = QueueManager()
