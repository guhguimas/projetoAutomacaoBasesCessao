import pandas as pd
from app.core.data_loader import DataLoader
from app.core.processors.step1_builder import Step1Builder
from app.config.schemas import FILE_SCHEMAS


def log_console(message, level="INFO"):
    print(f"[{level}] {message}")


if __name__ == "__main__":
    path_x = r"C:\Users\GUH\Documents\# Gustavo\# Desenvolvimentos\_Docs\_Cessao\CONTROLE_PLANILHA_CESSÃO 2025 - VINI AKRK+DIG 21.01.xlsx"
    path_front_akrk = r"C:\Users\GUH\Documents\# Gustavo\# Desenvolvimentos\_Docs\_Cessao\BASE CRM DIG 23.01 TESTE.csv"
    path_front_dig = r"C:\Users\GUH\Documents\# Gustavo\# Desenvolvimentos\_Docs\_Cessao\BASE CRM DIG 23.01 TESTE.csv"

    loader = DataLoader(log_callback=log_console)

    df_x = loader.load_with_schema("cessao", path_x)
    df_front_akrk = loader.load_with_schema("frontAkrk", path_front_akrk)
    df_front_dig = loader.load_with_schema("frontDig", path_front_dig)

    df_front = pd.concat([df_front_akrk, df_front_dig], ignore_index=True)

    s = df_front["dsOperacaoCRM"].astype(str).str.strip().str.upper()

    alvos = ["CAPITAL", "DIG", "AKRK", "GRUPO AKRK", "GDC", "SEM CESSAO", "#N/D"]

    print("TOTAL FRONT:", len(df_front))
    print("MATCH exato:", s.isin(alvos).sum())
    print("MATCH contem termo:", s.str.contains("CAPITAL|DIG|AKRK|GRUPO AKRK|GDC|SEM CESSAO|#N/D", na=False).sum())
    print("EXEMPLOS que contem termo:", df_front.loc[s.str.contains("CAPITAL|DIG|AKRK|GRUPO AKRK|GDC|SEM CESSAO|#N/D", na=False), "dsOperacaoCRM"].head(20).tolist())


    print("SCHEMA frontDig rename:", FILE_SCHEMAS["frontDig"]["rename"])
    print("SCHEMA frontDig use:", FILE_SCHEMAS["frontDig"]["use"])
    
    # df_raw = loader.load(path_x)
    # for c in df_raw.columns:
    #     print(repr(c))

    builder = Step1Builder(log_callback=log_console)
    
    print("COLUNAS FRONT AKRK:", list(df_front_akrk.columns))
    print("COLUNAS FRONT DIG:", list(df_front_dig.columns))
    
    df_y = builder.build(df_x, df_front_akrk, df_front_dig)

    print("LINHAS FINAL (após filtros 1-4):", len(df_y))

    print("\n=== CHECAGEM DE COLUNAS ===")
    print("dsOperacaoFront em X:", "dsOperacaoFront" in df_x.columns)
    print("dsOperacaoCRM em FRONT AKRK:", "dsOperacaoCRM" in df_front_akrk.columns)
    print("dsOperacaoCRM em FRONT DIG:", "dsOperacaoCRM" in df_front_dig.columns)
    print("dsOperacao em Y:", "dsOperacao" in df_y.columns)

    print("\n=== AMOSTRA (3 LINHAS) ===")
    cols_x = ["nrCCB", "dsOperacaoFront"]
    if "dsOperacaoCRM" in df_x.columns:
        cols_x.append("dsOperacaoCRM")

    print("\nX (após merge dentro do builder, se você logar lá):")
    print(df_x[["nrCCB", "dsOperacaoFront"]].head(3))

    df_front = pd.concat([df_front_akrk, df_front_dig], ignore_index=True)
    df_front = df_front.drop_duplicates(subset=["nrCCB"])

    pct = df_x["nrCCB"].isin(df_front["nrCCB"]).mean()
    print("MATCH % nrCCB:", f"{pct:.2%}")

    print("\nY (resultado final):")
    print(df_y[["nrCCB", "dsOperacao"]].head(3))

    print("\n=== TAMANHOS ===")
    print("Linhas X:", len(df_x))
    print("Linhas Y:", len(df_y))

    print("HEAD FRONT DIG:\n", df_front_dig.head(3))
    print("UNIQUE dsOperacao FRONT (amostra):",df_front_dig["dsOperacaoCRM"].dropna().astype(str).head(20).tolist())


