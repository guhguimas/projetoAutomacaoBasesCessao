import os
import threading
import time
import traceback
import pandas as pd
from datetime import datetime
from enum import Enum
from app.logs.log_manager import LogManager
from uuid import uuid4
from app.core.data_loader import DataLoader, DataLoaderError
from app.config.robot_config import FILE_PLAN
from app.config.schemas import Y_DATE_COLUMNS, EXPORT_CSV_SEP, EXPORT_CSV_ENCODING
from app.core.processors.step1_builder import Step1Builder
from openpyxl import load_workbook


class RobotStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"
    ERROR = "error"


class RobotController:
    def __init__(self, log_callback=None, status_callback=None, finish_callback=None, progress_callback=None, file_manager=None):
        self.status = RobotStatus.IDLE
        self._stop_event = threading.Event()
        self.log = log_callback
        self.on_finish = finish_callback
        
        self.log_callback = log_callback
        self.status_callback = status_callback
        self._thread = None

        self.step1_builder = Step1Builder(
        log_callback=self._log,
        stop_callback=lambda: self._stop_event.is_set()
)
        self.log_manager = LogManager()
        self.execution_id = None

        self.progress_callback = progress_callback

        self.file_manager = file_manager
        
        self.loader = DataLoader(csv_encoding="utf-8", csv_sep=";", log_callback=self._log)
        
        self.dataframes = {}

        self.output_dir = os.path.join(os.getcwd(), "output")

    def _log(self, message, level="INFO"):
        if self.log_callback:
            self.log_callback(message, level)

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

            tb = traceback.format_exc()
            self._log("Erro inesperado (traceback completo):", "ERROR")
            self._log(tb, "ERROR")

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
        if not self.file_manager:
            raise DataLoaderError(f"FileManager não foi informado no robô.")
        
        missing = self.file_manager.get_missing_files()
        if missing:
            self._log(f"Arquivos ausentes: {', '.join(missing)}", "WARNING")
        else:
            self._log("Todos os arquivos foram selecionados.", "SUCCESS")

        for key, label, required in FILE_PLAN:
            if self._stop_event.is_set():
                return
            
            path = self.file_manager.files.get(key)
            if not path:
                self._log(f"Pulando (não selecionado): {label}", "WARNING")
                continue

            self._log(f"Carregando arquivo: {label}", "INFO")
            
            df = self.loader.load_with_schema(key, path)
            self.dataframes[key] = df
            self._log(f"Concluído: {label} | {df.shape[0]} linhas, {df.shape[1]} colunas", "SUCCESS")
        
    def _step_process_data(self):
        self._log("Etapa 2: Processando dados", "INFO")

        if self._stop_event.is_set():
            return

        df_x = self.dataframes.get("cessao")
        df_front_akrk = self.dataframes.get("frontAkrk")
        df_front_dig = self.dataframes.get("frontDig")

        if df_x is None or df_x.empty:
            self._log("Base 'cessao' não foi carregada (self.dataframes['cessao']).", "ERROR")
            return

        if (df_front_akrk is None or df_front_akrk.empty) and (df_front_dig is None or df_front_dig.empty):
            self._log("FRONT AKRK e FRONT DIG não foram carregados.", "ERROR")
            return

        df_y = self.step1_builder.build(
            df_x=df_x,
            df_front_akrk=df_front_akrk if df_front_akrk is not None else pd.DataFrame(),
            df_front_dig=df_front_dig if df_front_dig is not None else pd.DataFrame()
        )

        self.dataframes["y"] = df_y
        self._log(f"Etapa 2 concluída: Planilha Y gerada | {len(df_y)} linhas", "SUCCESS")
        
    def _step_validate(self):
        self._log("Etapa 3: Validando contratos")
        time.sleep(1)

        if self._stop_event.is_set():
            return
        
    def _step_export(self):
        self._log("Etapa 4: Exportando Planilha Cessao", "INFO")

        if self._stop_event.is_set():
            return

        df_y = self.dataframes.get("y")
        if df_y is None or df_y.empty:
            self._log("Nenhuma planilha Y encontrada para exportar.", "WARNING")
            return

        os.makedirs(self.output_dir, exist_ok=True)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(self.output_dir, f"cessao_Y_{stamp}.csv")
        xlsx_path = os.path.join(self.output_dir, f"cessao_Y_{stamp}.xlsx")

        for col in Y_DATE_COLUMNS:
            if col in df_y.columns:
                df_y[col] = pd.to_datetime(df_y[col], errors="coerce", dayfirst=True).dt.date

        df_y.to_csv(
            csv_path,
            sep=EXPORT_CSV_SEP,
            encoding=EXPORT_CSV_ENCODING,
            index=False,
            date_format="%d/%m/%Y"
        )
        self._log(f"CSV exportado: {csv_path}", "SUCCESS")

        df_y.to_excel(
        xlsx_path,
        index=False,
        engine="openpyxl"
        )
        self._log(f"Excel exportado: {xlsx_path}", "SUCCESS")

        wb = load_workbook(xlsx_path)
        ws = wb.active

        # mapeia nome da coluna -> índice (1-based)
        headers = [cell.value for cell in ws[1]]
        col_index = {name: i+1 for i, name in enumerate(headers)}

        date_cols = [c for c in Y_DATE_COLUMNS if c in col_index]

        for col_name in date_cols:
            idx = col_index[col_name]
            for row in range(2, ws.max_row + 1):
                cell = ws.cell(row=row, column=idx)
                # Se a célula já for datetime/date, aplica formato
                cell.number_format = "DD/MM/YYYY"

        wb.save(xlsx_path)
        
    def _step_finalize(self):
        self._log("Etapa 5; Finalização")
        time.sleep(1)

        if self._stop_event.is_set():
            return
    
    def _progress(self, current_step, total_steps, message):
        if self.progress_callback:
            self.progress_callback(current_step, total_steps, message)

    def _check_files(self):
        missing_required = []

        for key, label, required in FILE_PLAN:
            path = self.file_manager.files.get(key)
            if required and not path:
                missing_required.append(f"{label} ({key})")
        
        return missing_required