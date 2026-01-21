from __future__ import annotations
import os
import pandas as pd


class DataLoaderError(Exception):
    pass

class DataLoader:
    def __init__(self, csv_encoding="utf-8", csv_sep=";"):
        self.csv_encoding = csv_encoding
        self.csv_sep = csv_sep

    def load(self, path: str) -> pd.DataFrame:
        if not path:
            raise DataLoaderError("Caminho vazio ou inválido.")

        ext = os.path.splitext(path)[1].lower()

        if ext in [".xlsx", ".xls"]:
            return self._load_excel(path)

        if ext == ".csv":
            return self._load_csv(path)

        raise DataLoaderError(f"Extensão não suportada: {ext}")
    
    def _load_csv(self, path: str) -> pd.DataFrame:
        encodings_to_try = [self.csv_encoding, "utf-8-sig", "cp1252", "latin1"]

        last_error = None

        for enc in encodings_to_try:
            try:
                df = pd.read_csv(
                    path,
                    sep=self.csv_sep,
                    encoding=enc,
                    dtype=str,
                    keep_default_na=False
                )
                return df
            except UnicodeDecodeError as e:
                last_error = e
                continue
            except Exception as e:
                raise DataLoaderError(f"Falha ao ler CSV (erro não relacionado a encoding): {os.path.basename(path)} | {e}") from e
        raise DataLoaderError(f"Falha ao ler CSV: {os.path.basename(path)} | encoding não compatível. Último erro: {last_error}") from last_error

    def _load_excel(self, path: str) -> pd.DataFrame:
        try:
            df = pd.read_excel(
                path,
                dtype=str,
                engine="openpyxl"
            )
            df = df.fillna("")
            return df
        except Exception as e:
            raise DataLoaderError(f"Fala ao ler Excel: {os.path.basename(path)} | {e}") from e
