import json
import os
from datetime import datetime

class LogManager:
    def __init__(self, log_dir="logs", filename="cessao_prime_logs.json"):
        self.log_dir = log_dir
        self.filepath = os.path.join(log_dir, filename)

        self._ensure_log_file()

    def _ensure_log_file(self):
        os.makedirs(self.log_dir, exist_ok=True)

        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump({"executions" :[]}, f, indent=4, ensure_ascii=False)
    
    def start_execution(self):
        execution_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        execution = {
            "execution_id": execution_id,
            "status": "RUNNING",
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": None,
            "logs": []
        }

        data = self._read_file()
        data["executions"].append(execution)
        self._write_file(data)

        return execution_id

    def add_log(self, execution_id, level, message):
        data = self._read_file()

        for execution in data["executions"]:
            if execution["execution_id"] == execution_id:
                execution["logs"].append({
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "level": level,
                    "message": message
                })

                break

        self._write_file(data)

    def finish_execution(self, execution_id, status):
        data = self._read_file()

        for execution in data["executions"]:
            if execution["execution_id"] == execution_id:
                execution["status"] = status
                execution["finished_at"] = datetime.now().strftime("%Y-%m-%D %H:%M:%S")
                
                break

        self._write_file(data)


    def _read_file(self):
        with open(self.filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_file(self, data):
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
