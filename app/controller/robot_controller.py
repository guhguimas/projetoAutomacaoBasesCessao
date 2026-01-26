import os
import threading
import time
import traceback
import pandas as pd
import numpy as np
from datetime import datetime
from enum import Enum
from app.logs.log_manager import LogManager
from uuid import uuid4
from app.core.data_loader import DataLoader, DataLoaderError
from app.config.robot_config import FILE_PLAN
from app.config.schemas import Y_DATE_COLUMNS, EXPORT_CSV_SEP, EXPORT_CSV_ENCODING, DEFAULT_MISSING_VALUE
from app.core.processors.step1_builder import Step1Builder
from openpyxl import load_workbook


class RobotStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"
    ERROR = "error"

def format_vl_taxa_cessao(series: pd.Series, max_pct: float = 3.99) -> pd.Series:
    raw = series.astype(str).str.strip()

    had_pct = raw.str.contains("%", na=False)

    raw = raw.replace({"#N/D": "", "nan": "", "None": "", "": ""})
    raw = raw.str.replace("%", "", regex=False)
    raw = raw.str.replace("\u00a0", " ", regex=False) 
    raw = raw.str.replace(",", ".", regex=False)
    raw = raw.str.replace(r"[^0-9\.\-]+", "", regex=True)

    num = pd.to_numeric(raw, errors="coerce")

    pct = pd.Series(index=num.index, dtype="float64")
    pct[:] = float("nan")

    pct = pct.where(~had_pct, num)

    no_pct = ~had_pct
    pct = pct.where(~(no_pct & (num > 1)), num)
    pct = pct.where(~(no_pct & (num > 0) & (num <= 1)), num * 100)

    for _ in range(6):
        mask = pct.notna() & (pct > max_pct) & (pct <= 1000)
        if not mask.any():
            break
        pct = pct.where(~mask, pct / 10)

    pct = pct.where(pct.notna() & (pct >= 0) & (pct <= 100), float("nan"))

    return pct.round(2).map(lambda x: f"{x:.2f}" if pd.notna(x) else "#N/D")

def format_vl_taxa_cessao(series: pd.Series, max_pct: float = 3.99) -> pd.Series:
    raw0 = series.astype(str).str.strip()

    had_pct = raw0.str.contains("%", na=False)

    raw = raw0.replace({"#N/D": "", "nan": "", "None": "", "": ""})
    raw = raw.str.replace("\u00a0", " ", regex=False)
    raw = raw.str.replace("%", "", regex=False)
    raw = raw.str.replace(",", ".", regex=False)
    raw = raw.str.replace(r"[^0-9\.\-]+", "", regex=True)

    num = pd.to_numeric(raw, errors="coerce")

    pct = pd.Series(np.nan, index=num.index, dtype="float64")

    pct.loc[had_pct] = num.loc[had_pct]

    no_pct = ~had_pct

    m_frac = no_pct & num.notna() & (num >= 0) & (num <= 1)
    pct.loc[m_frac] = (num.loc[m_frac] * 100)

    m_pts = no_pct & num.notna() & (num > 1)
    pct.loc[m_pts] = num.loc[m_pts]

    for _ in range(4):
        mask = (~np.isnan(pct)) & (pct > max_pct) & (pct <= 1000)
        if not mask.any():
            break
        pct.loc[mask] = pct.loc[mask] / 10

    pct = pct.where((pct >= 0) & (pct <= 100), np.nan)

    out = pd.Series(np.where(np.isnan(pct), "#N/D", np.round(pct, 2)), index=num.index)
    out = out.map(lambda x: x if x == "#N/D" else f"{float(x):.2f}")
    return out

class RobotController:
    def __init__(self, log_callback=None, status_callback=None, finish_callback=None, progress_callback=None, file_manager=None, export_format="xlsx"):
        self.status = RobotStatus.IDLE
        self._stop_event = threading.Event()
        self.log = log_callback
        self.on_finish = finish_callback
        
        self.log_callback = log_callback
        self.status_callback = status_callback
        self._thread = None

        self.step1_builder = Step1Builder()
        log_callback=self._log,
        stop_callback=lambda: self._stop_event.is_set()

        self.log_manager = LogManager()
        self.execution_id = None

        self.progress_callback = progress_callback

        self.file_manager = file_manager
        
        self.loader = DataLoader(csv_encoding="utf-8", csv_sep=";", log_callback=self._log)
        
        self.dataframes = {}

        self.output_dir = os.path.join(os.getcwd(), "output")

        self.export_format = export_format

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
            raise DataLoaderError("FileManager não foi informado no robô.")

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

            self._log(f"Concluído: {label} | {df.shape[0]} linhas, {df.shape[1]} colunas","SUCCESS")
        
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

        df_export = df_y.copy()

        for col in Y_DATE_COLUMNS:
            if col in df_export.columns:
                dt = pd.to_datetime(df_export[col], errors="coerce", dayfirst=True)
                df_export[col] = dt.dt.date

        if "vlTaxaCessao" in df_export.columns:
            df_export["vlTaxaCessao"] = format_vl_taxa_cessao(df_export["vlTaxaCessao"], max_pct=3.99)

        os.makedirs(self.output_dir, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if self.export_format == "csv":
            csv_path = os.path.join(self.output_dir, f"cessao_Y_{stamp}.csv")
            df_export.to_csv(csv_path, sep=EXPORT_CSV_SEP, encoding=EXPORT_CSV_ENCODING, index=False)
            self._log(f"CSV exportado: {csv_path}", "SUCCESS")

        elif self.export_format == "xlsx":
            xlsx_path = os.path.join(self.output_dir, f"cessao_Y_{stamp}.xlsx")
            df_export.to_excel(xlsx_path, index=False, engine="openpyxl")
            self._log(f"Excel exportado: {xlsx_path}", "SUCCESS")

        else:
            self._log(f"Formato de exportação inválido: {self.export_format}", "ERROR")
        
        if self.export_format == "csv":
            csv_path = os.path.join(self.output_dir, f"cessao_Y_{stamp}.csv")
            df_export.to_csv(
                csv_path,
                sep=EXPORT_CSV_SEP,
                encoding=EXPORT_CSV_ENCODING,
                index=False
            )
            self._log(f"CSV exportado: {csv_path}", "SUCCESS")
            return

        xlsx_path = os.path.join(self.output_dir, f"cessao_Y_{stamp}.xlsx")
        df_export.to_excel(
            xlsx_path,
            index=False,
            engine="openpyxl"
        )
        self._log(f"Excel exportado: {xlsx_path}", "SUCCESS")
        
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

    def _parse_taxa_to_points(self, series: pd.Series) -> pd.Series:
        s = series.astype(str).str.strip()

        s = s.replace({"#N/D": "", "nan": "", "None": "", "": ""})

        s = s.str.replace("%", "", regex=False)

        s = s.str.replace("\u00a0", " ", regex=False).str.strip()

        has_comma = s.str.contains(",", na=False)
        s.loc[has_comma] = (
            s.loc[has_comma]
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )

        s = s.str.replace(r"[^0-9\.\-]+", "", regex=True)

        num = pd.to_numeric(s, errors="coerce")

        num = num.where(num.isna() | (num > 1), num * 100)

        num = num.where(num.isna() | (num <= 100), pd.NA)

        return num.round(2)

    def _taxa_to_points_str(self, series: pd.Series, max_pct: float = 3.99) -> pd.Series:
        raw = series.astype(str).str.strip()

        raw = raw.replace({"#N/D": "", "nan": "", "None": "", "": ""})
        raw = raw.str.replace("%", "", regex=False)
        raw = raw.str.replace(",", ".", regex=False)
        raw = raw.str.replace(r"[^0-9\.\-]+", "", regex=True)

        num = pd.to_numeric(raw, errors="coerce")

        frac = (num > 0) & (num < 1)
        too_big_as_pct = frac & ((num * 100) > max_pct)

        for _ in range(3):
            adj = (num > 0) & (num < 1) & ((num * 100) > max_pct)
            if not adj.any():
                break
            num = num.where(~adj, num / 10)

        pp = num > 1
        for _ in range(3):
            adj = (num > max_pct) & (num <= 100)
            if not adj.any():
                break
            num = num.where(~adj, num / 10)

        num = num.where(num.isna() | (num > 1), num * 100)

        num = num.where(num.isna() | (num <= 100), pd.NA)

        out = num.round(2).map(lambda x: f"{x:.2f}" if pd.notna(x) else "#N/D")
        return out

