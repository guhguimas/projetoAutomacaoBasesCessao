import pandas as pd
import re
from app.config.columns_config import COLS_X, COLS_FRONT
from app.config.schemas import DEFAULT_MISSING_VALUE, Y_COLUMNS_FULL
from app.config.rules_config import DEFAULT_MISSING_VALUE, ALLOWED_CRM_OPERATIONS, EXCLUDED_CONVENIOS
from app.config.schemas import Y_DATE_COLUMNS

class Step1Builder:
    def __init__(self, logger=None, stop_check=None, log_callback=None, stop_callback=None):
        self.logger = logger
        self.stop_chek = stop_check
        self.log_callback = log_callback
        self.stop_callback = stop_callback

    def _log(self, msg, level="INFO"):
        if self.logger:
            self.logger(msg, level)
        
        if self.log_callback:
            self.log_callback(msg, level)

    def _stop(self):
        if self.stop_callback:
            return bool(self.stop_callback())
        return bool(self.stop_chek and self.stop_chek())
    
    def _norm_contract(self, s: pd.Series) -> pd.Series:
        s = s.astype(str).str.strip()
        s = s.str.replace(".0", "", regex=False)
        return s
    
    def build(self, df_x: pd.DataFrame, df_front_akrk: pd.DataFrame, df_front_dig: pd.DataFrame) -> pd.DataFrame:
        self._log("Etapa 1: iniciando (BASE CESSAO + FRONT AKRK + FRONT DIG)", "INFO")

        df_x = df_x.copy()

        mask_invest = df_x["nrCCB"].astype(str).str.contains("CCB INVESTIDOR", na=False)
        df_x.loc[mask_invest, "nrContratoCred"] = (
            df_x.loc[mask_invest, "nrContratoCred"].astype(str).str.replace("-", "", regex=False)
        )
        self._log(f"Regra CCB aplicada: {mask_invest.sum()} linhas", "INFO")

        if self._stop():
            return pd.DataFrame()

        df_front = pd.concat([df_front_akrk, df_front_dig], ignore_index=True)

        df_front["nrCCB"] = df_front["nrCCB"].astype(str).str.strip()
        df_x["nrCCB"] = df_x["nrCCB"].astype(str).str.strip()

        col_op = "dsOperacaoCRM" if "dsOperacaoCRM" in df_front.columns else "dsOperacao"
        df_front = df_front[["nrCCB", col_op]].copy()
        df_front = df_front.rename(columns={col_op: "dsOperacaoCRM"})
        df_front = df_front.drop_duplicates(subset=["nrCCB"], keep="first")

        df_x = df_x.merge(df_front, how="left", on="nrCCB")

        matched = df_x["dsOperacaoCRM"].notna().sum()
        total = len(df_x)
        self._log(f"Match CRM por nrCCB: {matched}/{total} ({matched/total:.2%})", "INFO")

        df_x["dsOperacaoCRM"] = df_x["dsOperacaoCRM"].fillna(DEFAULT_MISSING_VALUE)

        df_x["dsOperacaoCRM"] = df_x["dsOperacaoCRM"].astype(str).str.strip()
        df_x.loc[df_x["dsOperacaoCRM"].eq(""), "dsOperacaoCRM"] = DEFAULT_MISSING_VALUE

        s = df_x["dsOperacaoCRM"].astype(str)
        s = s.str.replace("\u00a0", " ", regex=False)      # nbsp
        s = s.str.replace("ª", "", regex=False)
        s = s.str.upper()
        s = s.str.replace(r"\s+", " ", regex=True).str.strip()

        s = s.replace({"": DEFAULT_MISSING_VALUE.upper(), "NAN": DEFAULT_MISSING_VALUE.upper()})
        s = s.fillna(DEFAULT_MISSING_VALUE.upper())

        df_x["dsOperacaoCRM_norm"] = s

        before = len(df_x)
        df_x = df_x[df_x["dsOperacaoCRM_norm"].isin(ALLOWED_CRM_OPERATIONS)].copy()
        self._log(f"Filtro operação CRM (EXATO) aplicado: {before} -> {len(df_x)}", "INFO")

        self._log(f"Sem match no FRONT (viraram #N/D): {(df_x['dsOperacaoCRM_norm'] == DEFAULT_MISSING_VALUE.upper()).sum()}", "INFO")

        if len(df_x) > 0:
            self._log(
                "Top 30 dsOperacaoCRM_norm:\n" +
                df_x["dsOperacaoCRM_norm"].value_counts().head(30).to_string(),
                "INFO"
            )
        else:
            self._log("Após filtro EXATO, df_x ficou vazio.", "WARNING")

        if self._stop():
            return pd.DataFrame()
        
        df_x["dsConvenio"] = df_x["dsConvenio"].astype(str).str.strip()
        mask_excluded = df_x["dsConvenio"].str.upper().isin(EXCLUDED_CONVENIOS)
        before = len(df_x)
        df_x = df_x[~mask_excluded].copy()
        df_x = df_x.reset_index(drop=True)
        self._log(f"Filtro Convenio Cessao aplicado: {before} -> {len(df_x)}", "INFO")

        if self._stop():
            return pd.DataFrame()

        df_x["vlTaxaCessao"] = self._normalize_percent_to_fraction(df_x["vlTaxaCessao"])
        df_y = pd.DataFrame(DEFAULT_MISSING_VALUE, index=range(len(df_x)), columns=Y_COLUMNS_FULL)
        
        df_y["nrCCB"] = df_x["nrCCB"]
        df_y["dtCessao"] = df_x["dtCessao"]
        df_x["dsOperacaoFront"] = df_x["dsOperacaoFront"].astype(str).str.replace("ª", "", regex=False).str.strip()
        df_x.loc[df_x["dsOperacaoFront"].eq("") | df_x["dsOperacaoFront"].str.lower().eq("nan"), "dsOperacaoFront"] = DEFAULT_MISSING_VALUE
        df_y["dsOperacao"] = df_x["dsOperacaoFront"]

        df_y["dsFundo"] = df_x["dsFundo"]
        df_y["dsConvenio"] = df_x["dsConvenio"]
        df_y["dsOrigem"] = df_x["dsOrigem"]
        df_y["vlTaxaCessao"] = df_x["vlTaxaCessao"]

        nr_contrato = df_x["nrContratoCred"].astype(str).str.strip()
        nr_ccb = df_x["nrCCB"].astype(str).str.strip()
        mask_hifen = nr_contrato.eq("-")
        nr_contrato = nr_contrato.where(~mask_hifen, nr_ccb.str.slice(0, 9))
        nr_contrato = nr_contrato.replace({"": DEFAULT_MISSING_VALUE, "nan": DEFAULT_MISSING_VALUE}).fillna(DEFAULT_MISSING_VALUE)

        df_y["nrContrato"] = nr_contrato

        df_y["cnpj"] = df_x["cnpj"]
        df_y["codTabelas"] = df_x["codTabelas"]
        df_y["tabela"] = df_x["tabela"]

        df_y["dtAverbacao"] = df_x.get("dtAverbacao", DEFAULT_MISSING_VALUE)
        df_y["dtPrimeiroVencimentoCessao"] = df_x.get("dtPrimeiroVencimentoCessao", DEFAULT_MISSING_VALUE)
        df_y["dtPrimeiroVencimentoAverbacao"] = df_x.get("dtPrimeiroVencimentoAverbacao", DEFAULT_MISSING_VALUE)

        self._log(f"nrContratoCred '-' substituídos por LEFT(nrCCB,9): {mask_hifen.sum()}", "INFO")
        self._log("Planilha Y inicial montada (layout + campos básicos)", "SUCCESS")
        
        for col in Y_DATE_COLUMNS:
            if col in df_y.columns:
                df_y[col] = self._normalize_date_only(df_y[col])
        
        df_y = df_y.fillna(DEFAULT_MISSING_VALUE)
        return df_y
    
    def _normalize_date_only(self, series: pd.Series) -> pd.Series:
        if pd.api.types.is_datetime64_any_dtype(series):
            return series.dt.date

        s = series.astype(str).str.strip()

        iso_mask = s.str.match(r"^\d{4}-\d{2}-\d{2}", na=False)

        out = pd.Series([pd.NaT] * len(s), index=s.index, dtype="datetime64[ns]")

        if iso_mask.any():
            out.loc[iso_mask] = pd.to_datetime(s.loc[iso_mask], errors="coerce")

        if (~iso_mask).any():
            out.loc[~iso_mask] = pd.to_datetime(s.loc[~iso_mask], errors="coerce", dayfirst=True)

        return out.dt.date

    def _normalize_percent_to_fraction(self, series: pd.Series) -> pd.Series:
        s = series.astype(str).str.strip()
        s = s.str.replace("%", "", regex=False)
        s = s.str.replace(".", "", regex=False)
        s = s.str.replace(",", ".", regex=False)
        
        num = pd.to_numeric(s, errors="coerce")
        return num / 100

