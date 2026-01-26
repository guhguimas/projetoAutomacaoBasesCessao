import traceback
import sys
from pathlib import Path
import pandas as pd
from app.core.data_loader import DataLoader
from app.core.processors.step1_builder import Step1Builder

def log_console(message, level="INFO"):
    print(f"[{level}] {message}")

if __name__ == "__main__":
    try:
        print(">>> INICIO TESTE: test_modelo_y.py", flush=True)  
        path_x = r"C:\Users\GUH\Documents\# Gustavo\# Desenvolvimentos\_Docs\_Cessao\CONTROLE_PLANILHA_CESSÃO 2025 - VINI AKRK+DIG 21.01.xlsx"
        path_front_akrk = r"C:\Users\GUH\Documents\# Gustavo\# Desenvolvimentos\_Docs\_Cessao\BASE CRM DIG 23.01 TESTE.csv"
        path_front_dig = r"C:\Users\GUH\Documents\# Gustavo\# Desenvolvimentos\_Docs\_Cessao\BASE CRM DIG 23.01 TESTE.csv"

        for p in [path_x, path_front_akrk, path_front_dig]:
            if not Path(p).exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {p}")
        print(">>> Arquivos encontrados. Iniciando loader...", flush=True)
    
        loader = DataLoader(log_callback=log_console)

        print(">>> carregando X...", flush=True)
        df_x = loader.load_with_schema("cessao", path_x)
        print(">>> carregando FRONT AKRK...", flush=True)
        df_front_akrk = loader.load_with_schema("frontAkrk", path_front_akrk)
        print(">>> carregando FRONT DIG...", flush=True)
        df_front_dig = loader.load_with_schema("frontDig", path_front_dig)

        builder = Step1Builder(log_callback=log_console)
        print(">>> rodando Step1Builder...", flush=True)
        df_y = builder.build(df_x, df_front_akrk, df_front_dig)

        print(">>> Y gerado. linhas:", len(df_y), flush=True)    
        
        cols_fake = ["nrCpf", "dsNome", "vlPrestacao", "nrPrazo", "dsTipoOperacao", "dsEsteira", "dsConsignataria"]

        for c in cols_fake:
            if c not in df_y.columns:
                df_y[c] = "#N/D"

        df_sample = df_y.head(30000).copy()

        output_path = r"C:\Users\GUH\Documents\# Gustavo\# Desenvolvimentos\_Docs\_Cessao\ModeloTeste.xlsx"
        df_sample.to_excel(output_path, index=False)
        print("Arquivo modelo gerado em:", output_path)

        print(">>> FIM TESTE: OK", flush=True)

    except Exception as e:
        print("\n!!! ERRO NO TESTE !!!", flush=True)
        print("Tipo:", type(e).__name__, flush=True)
        print("Msg :", str(e), flush=True)
        print("\n--- TRACEBACK COMPLETO ---", flush=True)
        traceback.print_exc()
        sys.exit(1)

def log_console(message, level="INFO"):
    print(f"[{level}] {message}", flush=True)