# app/config/schemas.py

# ============================================================
# SCHEMAS: colunas mínimas por arquivo + renomeação canônica
# ============================================================

FILE_SCHEMAS = {
    # ---------------------------
    # Planilha Base Cessão (X)
    # ---------------------------
    "cessao": {
        "use": [
            "DATA CESSÃO",      # B
            "CCB INVESTIDOR",   # E
            "CONTRATO CRED",    # F
            "OPERACAO",         # H
            "FUNDO",            # I
            "cnpj",             # J
            "COD TABELAS",      # L
            "TABELA",           # M
            "CONVENIO",         # Q
            "ORIGEM",           # S
            "TAXA CESSÃO",      # V
        ],
        "rename": {
            "DATA CESSÃO": "dtCessao",
            "CCB INVESTIDOR": "nrCCB",
            "CONTRATO CRED": "nrContratoCred",
            "OPERACAO": "dsOperacaoFront",
            "FUNDO": "dsFundo",
            "cnpj": "cnpj",
            "COD TABELAS": "codTabelas",
            "TABELA": "tabela",
            "CONVENIO": "dsConvenio",
            "ORIGEM": "dsOrigem",
            "TAXA CESSÃO": "vlTaxaCessao",
        },
        "key_field": "nrContratoCred",  # chave pra iniciar o fluxo (depois vira nrContrato em alguns casos)
    },

    # ---------------------------
    # FRONT (A/B)
    # ---------------------------
    "frontAkrk": {
        "use": ["nrCCB", "dsOperacao"],  # B e K
        "rename": {
            "nrCCB": "nrCCB",
            "dsOperacao": "dsOperacaoCRM",
        },
        "key_field": "nrCCB",
    },
    "frontDig": {
        "use": ["nrCCB", "dsOperacao"],
        "rename": {
            "nrCCB": "nrCCB",
            "dsOperacao": "dsOperacaoCRM",
        },
        "key_field": "nrCCB",
    },

    # ---------------------------
    # Iniciados (C/D)
    # ---------------------------
    "credAkrk": {
        "use": [
            "Codigo Credbase",  # A
            "Esteira",          # J
            "Tipo",             # L
            "Cliente",          # P
            "CPF",              # R
            "Convenio",         # T
            "Banco",            # U
            "Parcela",          # Y
            "Prazo",            # Z
        ],
        "rename": {
            "Codigo Credbase": "nrContrato",
            "Esteira": "dsEsteira",
            "Tipo": "dsTipoOperacao",
            "Cliente": "dsNome",
            "CPF": "nrCpf",
            "Convenio": "dsConvenio",
            "Banco": "dsConsignataria",
            "Parcela": "vlPrestacao",
            "Prazo": "nrPrazo",
        },
        "key_field": "nrContrato",
    },
    "credDig": {
        "use": [
            "Codigo Credbase",
            "Esteira",
            "Tipo",
            "Cliente",
            "CPF",
            "Convenio",
            "Banco",
            "Parcela",
            "Prazo",
        ],
        "rename": {
            "Codigo Credbase": "nrContrato",
            "Esteira": "dsEsteira",
            "Tipo": "dsTipoOperacao",
            "Cliente": "dsNome",
            "CPF": "nrCpf",
            "Convenio": "dsConvenio",
            "Banco": "dsConsignataria",
            "Parcela": "vlPrestacao",
            "Prazo": "nrPrazo",
        },
        "key_field": "nrContrato",
    },

    # ---------------------------
    # Averbados (G/H)
    # ---------------------------
    "averbadosAkrk": {
        "use": [
            "Codigo Credbase",  # A
            "Data Averbação",   # O
            "1º Vencimento",    # P
        ],
        "rename": {
            "Codigo Credbase": "nrContrato",
            "Data Averbação": "dtAverbacao",
            "1º Vencimento": "dtPrimeiroVencimentoAverbacao",
        },
        "key_field": "nrContrato",
    },
    "averbadosDig": {
        "use": [
            "Codigo Credbase",
            "Data Averbação",
            "1º Vencimento",
        ],
        "rename": {
            "Codigo Credbase": "nrContrato",
            "Data Averbação": "dtAverbacao",
            "1º Vencimento": "dtPrimeiroVencimentoAverbacao",
        },
        "key_field": "nrContrato",
    },

    # ---------------------------
    # Operações Realizadas (E)
    # ---------------------------
    "integradosFunc": {
        "use": [
            "NR_OPER",     # B
            "CPF",         # D
            "CLIENTE",     # F
            "PARC",        # J
            "VLR_OP",      # L
            "VLR_FINAL",   # O
            "VLR_PARC",    # W
            "PRIM_VCTO",   # X
            "COD_PRODUTO", # AJ
            "PRODUTO",     # AK
            "ORIGEM_3",    # AS
            "ORIGEM_4",    # AU
        ],
        "rename": {
            "NR_OPER": "nrCCB",
            "CPF": "nrCpf",
            "CLIENTE": "dsNome",
            "PARC": "vlPrestacao",
            "VLR_OP": "vlPrincipal",
            "VLR_FINAL": "vlCessao",
            "VLR_PARC": "vlPrestacaoCalc",
            "PRIM_VCTO": "dtPrimeiroVencimentoCessao",
            "COD_PRODUTO": "codProduto",
            "PRODUTO": "dsProduto",
            "ORIGEM_3": "origem3",
            "ORIGEM_4": "origem4",
        },
        "key_field": "nrCCB",
    },

    # ---------------------------
    # RLE (F)
    # ---------------------------
    "esteirasFunc": {
        "use": [
            "Operação",    # B
            "MatrÍcula",   # BD
        ],
        "rename": {
            "Operação": "nrCCB",
            "MatrÍcula": "dsMatricula",
        },
        "key_field": "nrCCB",
    },
}

# ============================================================
# Cabeçalho final da Planilha Y (canônico)
# ============================================================
Y_COLUMNS = [
    "nrContrato",
    "nrCCB",
    "nrCpf",
    "dsMatricula",
    "dsNome",
    "vlPrestacao",
    "vlPrincipal",
    "vlCessao",
    "vlTaxaCessao",
    "nrPrazo",
    "dsOperacao",
    "dsFundo",
    "dsConvenio",
    "dtCessao",
    "dtPrimeiroVencimentoCessao",
    "dtAverbacao",
    "dtPrimeiroVencimentoAverbacao",
    "dsConsignataria",
    "dsOrigem",
    "dsTipoOperacao",
    "dsEsteira",
    "codFundo",
    "codConvenio",
    "codTipoOperacao",
    "codConsignataria",
    "Orbital",
    "StatusContrato",
]

# Export padrão (você já definiu)
EXPORT_CSV_ENCODING = "utf-8"
EXPORT_CSV_SEP = ";"
DEFAULT_MISSING_VALUE = "#N/D"

# ============================================================
# ALIASES: se o arquivo vier com nome diferente, padroniza
# ============================================================

COLUMN_ALIASES = {
    # RLE
    "Matrícula": "MatrÍcula",
    "Matricula": "MatrÍcula",

    # Base Cessão
    "Taxa Cessão": "TAXA CESSÃO",
    "TAXA CESSAO": "TAXA CESSÃO",
    "Taxa CESSÃO": "TAXA CESSÃO",
}

Y_EXTRA_COLUMNS = [
    "cnpj",
    "codTabelas",
    "tabela",
    "codProduto",
    "origem_3",
    "bancos_funcao"
]

Y_COLUMNS_FULL = Y_COLUMNS + Y_EXTRA_COLUMNS

ALLOWED_CRM_OPERATIONS = {
    "CAPITAL",
    "DIG",
    "AKRK",
    "GRUPO AKRK",
    "GDC",
    "SEM CESSAO",
    DEFAULT_MISSING_VALUE.upper(),  # "#N/D"
}

Y_DATE_COLUMNS = [
    "dtCessao",
    "dtPrimeiroVencimentoCessao",
    "dtAverbacao",
    "dtPrimeiroVencimentoAverbacao",
]

MAX_TAXA_CESSAO_PCT = 10.0