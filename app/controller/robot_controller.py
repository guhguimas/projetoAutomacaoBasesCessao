import threading
import time
from datetime import datetime
from enum import Enum

class RobotStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"
    ERROR = "error"


class RobotController:
    def __init__(self, log_callback=None):
        self.log_callback = log_callback
        self._thread = None
        self._running = False
        self.status = "IDLE"

    def _log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_message =f"[{timestamp}] [{level}] {message}"

        if self.log_callback:
            self.log_callback(log_message)

    def start(self):
        if self._running:
            self._log("Robô já está em execução", "WARNING")
            return
        
        self._running = True
        self.status = "RUNNING"

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    
    def stop(self):
        if not self._running:
            self._log("Robô não está em execução", "WARNING")
            return
        
        self._log("Solicitação de parada recebida", "INFO")
        self._running = False
        self.status = "STOPPED"

    def _run(self):
        try:
            self.status = "RUNNING"
            self._log("Iniciando processamento do robô", "SUCCESS")

            self._step_load_files()
            if not self._running: return

            self._step_process_data()
            if not self._running: return
            
            self._step_validate()
            if not self._running: return
            
            self._step_export()
            if not self._running: return
            
            self._step_finalize()

            self.status = "FINISHED"
            self._log("Processamento finalizado com sucesso", "SUCCESS")
        
        except Exception as e:
            self.status = "ERROR"
            self._log(f"Erro inesperado: {e}", "ERROR")

        finally:
            if not self._running and self.status != "ERROR":
                self._log("Processamento interrompido pelo usuário", "WARNING")
            
            self._running = False


    def _step_load_files(self):
        if not self._running:
            return
            
        self._log("Etapa 1: Carregando arquivos")
        time.sleep(1)

    def _step_process_data(self):
        if not self._running:
            return
            
        self._log("Etapa 2: Processando dados")
        time.sleep(1)

    def _step_validate(self):
        if not self._running:
            return
            
        self._log("Etapa 3: Validando contratos")
        time.sleep(1)

    def _step_export(self):
        if not self._running:
            return
            
        self._log("Etapa 4: Exportando Planilha Cessao")
        time.sleep(1)

    def _step_finalize(self):
        if not self._running:
            return
            
        self._log("Etapa 5; Finalização")
        time.sleep(1)