import pandas as pd
from app.config.columns_config import COLS_X, COLS_FRONT

class Step1Builder:
    def __init__(self, logger=None, stop_check=None):
        self.logger = logger
        self.stop_chek = stop_check

    def _log(self, msg, level="INFO"):
        if self.logger:
            self.logger(msg, level)

    def _stop(self):
        return bool(self.stop_chek and self.stop_chek())
    
    def _norm_contract(self, s: pd.Series) -> pd.Series:
        s = s.astype(str).str.strip()
        s = s.str.replace(".0", "", regex=False)
        return s
    
    def build(self, df_x: pd.DataFrame, df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
        self._log("Etapa 1: iniciando (X + A + B)", "INFO")

        if self._stop():
            return pd.DataFrame()
        
        col_x_contrato = COLS_X["CCB INVESTIDOR"]
        col_x_operacao = COLS_X["OPERACAO"]

        df_x = df_x.copy()
        df_a = df_a.copy()
        df_b = df_b.copy()

        df_x[col_x_contrato] = self._norm_contract(df_x[col_x_contrato])
        df_a[COLS_FRONT["nrCCB"]] = self._norm_contract(df_a[COLS_FRONT["nrCCB"]])
        df_b[COLS_FRONT["nrCCB"]] = self._norm_contract(df_b[COLS_FRONT["nrCCB"]])

        mask_invest = df_x[col_x_operacao].astype(str).str.contains("CCB INVESTIDOR", na=False)
        df_x.loc[mask_invest, col_x_contrato] = df_x.loc[mask_invest, col_x_contrato].str.replace("-", "", regex=False)

        self._log(f"X: contratos normalizados | CCB ajustada: {mask_invest.sum()} linhas", "INFO")

        return df_x
