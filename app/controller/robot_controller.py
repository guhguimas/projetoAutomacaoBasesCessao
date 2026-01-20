import threading
import time
from datetime import datetime
from enum import Enum
from logs.log_manager import LogManager
from uuid import uuid4

class RobotStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"
    ERROR = "error"


class RobotController:
    def __init__(self, log_callback=None, status_callback=None, finish_callback=None, progress_callback=None):
        self.status = RobotStatus.IDLE
        self._stop_event = threading.Event()
        self.log = log_callback
        self.on_finish = finish_callback
        
        self.log_callback = log_callback
        self.status_callback = status_callback
        self._thread = None
        
        self.log_manager = LogManager()
        self.execution_id = None

        self.progress_callback = progress_callback

    def _log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_message =f"[{timestamp}] [{level}] {message}"

        if self.log_callback:
            self.log_callback(log_message)

        if self.execution_id:
            self.log_manager.add_log(
                execution_id=self.execution_id,
                level=level,
                message=message
            )

    def start(self):
        if self.status == RobotStatus.RUNNING:
            return

        self.execution_id = self.log_manager.start_execution()
        
        self.status = RobotStatus.RUNNING

        if self.status_callback:
            self.status_callback(self.status)
        
        self._stop_event.clear()

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    
    def stop(self):
        if self.status != RobotStatus.RUNNING:
            return

        self._log("Solicitação de parada recebida", "INFO")
        self._stop_event.set()
        self._set_status(RobotStatus.STOPPED)

    def _run(self):
        try:
            self._log("Iniciando processamento do robô", "SUCCESS")

            self._progress(1, 4, "Carregando arquivos")
            self._step_load_files()
            if self._stop_event.is_set(): 
                return

            self._progress(2, 4, "Processando dados")
            self._step_process_data()
            if self._stop_event.is_set(): 
                return
            
            self._progress(3, 4, "Validando contratos")
            self._step_validate()
            if self._stop_event.is_set(): 
                return
            
            self._progress(4, 4, "Exportando planilha")
            self._step_export()

            if not self._stop_event.is_set():
                self._set_status(RobotStatus.FINISHED)
                self._log("Processamento finalizado com sucesso", "SUCCESS")
        
        except Exception as e:
            self._set_status(RobotStatus.ERROR)
            self._log(f"Erro inesperado: {e}", "ERROR")

        finally:
            if self.status == RobotStatus.STOPPED:
                self.log_manager.finish_execution(self.execution_id, "STOPPED")
            elif self.status == RobotStatus.ERROR:
                self.log_manager.finish_execution(self.execution_id, "ERROR")
            else:
                self.log_manager.finish_execution(self.execution_id, "FINISHED")
                        
            if self.on_finish:
                self.on_finish()

    def _set_status(self, status: RobotStatus):
        self.status = status
        if self.status_callback:
            self.status_callback(status)

    def _step_load_files(self):
        self._log("Etapa 1: Carregando arquivos")
        time.sleep(1)

        if self._stop_event.is_set():
            return
        
    def _step_process_data(self):
        self._log("Etapa 2: Processando dados")
        time.sleep(1)

        if self._stop_event.is_set():
            return
        
    def _step_validate(self):
        self._log("Etapa 3: Validando contratos")
        time.sleep(1)

        if self._stop_event.is_set():
            return
        
    def _step_export(self):
        self._log("Etapa 4: Exportando Planilha Cessao")
        time.sleep(1)

        if self._stop_event.is_set():
            return
        
    def _step_finalize(self):
        self._log("Etapa 5; Finalização")
        time.sleep(1)

        if self._stop_event.is_set():
            return
    
    def _progress(self, current_step, total_steps, message):
        if self.progress_callback:
            self.progress_callback(current_step, total_steps, message)

