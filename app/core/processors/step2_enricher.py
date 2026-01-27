import pandas as pd
from app.config.schemas import DEFAULT_MISSING_VALUE, Y_COLUMNS_FULL, Y_DATE_COLUMNS


class Step2Enricher:
    def __init__(self, log_callback=None, stop_callback=None):
        self.log_callback = log_callback
        self.stop_callback = stop_callback

    def _log(self, msg, level="INFO"):
        if self.log_callback:
            self.log_callback(msg, level)

    def _stop(self):
        return bool(self.stop_callback and self.stop_callback())

    def _dedupe(self, df: pd.DataFrame, key: str) -> pd.DataFrame:
        if df is None or df.empty or key not in df.columns:
            return df
        return df.drop_duplicates(subset=[key], keep="first").copy()

    def _merge_one(self, y: pd.DataFrame, df_right: pd.DataFrame, on: str, cols: list[str], tag: str) -> pd.DataFrame:
        if df_right is None or df_right.empty:
            self._log(f"[{tag}] Base vazia. Merge ignorado.", "WARNING")
            return y

        if on not in y.columns:
            self._log(f"[{tag}] Coluna chave '{on}' não existe em Y.", "ERROR")
            return y

        if on not in df_right.columns:
            self._log(f"[{tag}] Coluna chave '{on}' não existe na base do merge.", "ERROR")
            return y

        left = y.copy()
        right = df_right.copy()

        # normaliza chaves
        left[on] = left[on].astype(str).str.strip().str.replace(".0", "", regex=False)
        right[on] = right[on].astype(str).str.strip().str.replace(".0", "", regex=False)

        # garante que as cols existam no right
        for c in cols:
            if c not in right.columns:
                right[c] = pd.NA

        # reduz right só no necessário + dedupe (1:1)
        before_r = len(right)
        right = right[[on] + cols].drop_duplicates(subset=[on], keep="last")
        dups = before_r - len(right)
        if dups > 0:
            self._log(f"[{tag}] Duplicados removidos em {on}: {dups}", "WARNING")

        merged = left.merge(right, how="left", on=on, suffixes=("", "_r"))

        # log de match (usando a coluna _r, e ignorando #N/D)
        probe = cols[0]
        probe_r = f"{probe}_r"
        if probe_r in merged.columns:
            matched = (~self._is_missing(merged[probe_r])).sum()
            self._log(f"[{tag}] Match por {on}: {matched}/{len(left)} ({matched/len(left):.2%})", "INFO")

        # preenche: se no Y está vazio (NaN OU #N/D), usa o valor do right
        for c in cols:
            c_r = f"{c}_r"
            if c_r in merged.columns:
                if c in merged.columns:
                    m = self._is_missing(merged[c]) & (~self._is_missing(merged[c_r]))
                    merged.loc[m, c] = merged.loc[m, c_r]
                else:
                    merged[c] = merged[c_r]
                merged.drop(columns=[c_r], inplace=True)

            # log real de preenchimento (sem contar #N/D)
            if c in merged.columns:
                filled = (~self._is_missing(merged[c])).sum()
                self._log(f"[{tag}] preenchido {c}: {filled}/{len(merged)}", "INFO")

        return merged

    def _fill_nd(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.fillna(DEFAULT_MISSING_VALUE)

    def build(
        self,
        df_y: pd.DataFrame,
        df_cred_akrk: pd.DataFrame | None,
        df_cred_dig: pd.DataFrame | None,
        df_averb_akrk: pd.DataFrame | None,
        df_averb_dig: pd.DataFrame | None,
        df_integrados: pd.DataFrame | None,
        df_esteiras: pd.DataFrame | None,
    ) -> pd.DataFrame:
        self._log("Etapa 2.1: Enriquecendo Y (INICIADOS/AVERBADOS/INTEGRADOS/ESTEIRAS)", "INFO")

        if df_y is None or df_y.empty:
            self._log("Y está vazia. Nada para enriquecer.", "WARNING")
            return df_y

        y = df_y.copy()

        frames_cred = []
        if df_cred_akrk is not None and not df_cred_akrk.empty:
            frames_cred.append(df_cred_akrk)
        if df_cred_dig is not None and not df_cred_dig.empty:
            frames_cred.append(df_cred_dig)

        df_cred = pd.concat(frames_cred, ignore_index=True) if frames_cred else pd.DataFrame()

        cred_cols = [
            "nrCpf", "dsNome", "vlPrestacao", "nrPrazo",
            "dsTipoOperacao", "dsEsteira", "dsConsignataria", "dsConvenio"
        ]
        
        y["nrCCB"] = self._norm_key_digits(y["nrCCB"])
        df_integrados["nrCCB"] = self._norm_key_digits(df_integrados["nrCCB"])
        df_esteiras["nrCCB"] = self._norm_key_digits(df_esteiras["nrCCB"])

        y = self._merge_one(y, df_cred, on="nrContrato", cols=cred_cols, tag="INICIADOS")

        frames_averb = []
        if df_averb_akrk is not None and not df_averb_akrk.empty:
            frames_averb.append(df_averb_akrk)
        if df_averb_dig is not None and not df_averb_dig.empty:
            frames_averb.append(df_averb_dig)

        df_averb = pd.concat(frames_averb, ignore_index=True) if frames_averb else pd.DataFrame()

        averb_cols = ["dtAverbacao", "dtPrimeiroVencimentoAverbacao"]
        y = self._merge_one(y, df_averb, on="nrContrato", cols=averb_cols, tag="AVERBADOS")

        df_integrados = df_integrados if df_integrados is not None else pd.DataFrame()
        integ_cols = [
            "vlPrincipal", "vlCessao", "vlPrestacaoCalc",
            "dtPrimeiroVencimentoCessao", "codProduto", "dsProduto",
            "origem3", "origem4"
        ]
        y = self._merge_one(y, df_integrados, on="nrCCB", cols=integ_cols, tag="INTEGRADOS")

        df_esteiras = df_esteiras if df_esteiras is not None else pd.DataFrame()
        esteira_cols = ["dsMatricula"]
        y = self._merge_one(y, df_esteiras, on="nrCCB", cols=esteira_cols, tag="ESTEIRAS")

        for c in Y_COLUMNS_FULL:
            if c not in y.columns:
                y[c] = DEFAULT_MISSING_VALUE
        y = y[Y_COLUMNS_FULL].copy()

        for c in Y_DATE_COLUMNS:
            if c in y.columns:
                dt = pd.to_datetime(y[c], errors="coerce", dayfirst=True)
                y[c] = dt.dt.date

        y = self._fill_nd(y)

        self._log("Etapa 2.1 concluída: Y enriquecida.", "SUCCESS")
        return y

    def _norm_key_digits(self, s: pd.Series) -> pd.Series:
        s = s.astype(str).str.strip()
        s = s.str.replace(".0", "", regex=False)
        s = s.str.replace(r"\D+", "", regex=True)   # fica só dígito
        return s

    def _is_missing(self, s: pd.Series) -> pd.Series:
    
        return s.isna() | (s.astype(str).str.strip() == DEFAULT_MISSING_VALUE)