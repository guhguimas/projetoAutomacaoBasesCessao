# app/config/rules_config.py

DEFAULT_MISSING_VALUE = "#N/D"

# Lista fechada do filtro (operações que você quer manter vindas do FRONT/CRM)
ALLOWED_CRM_OPERATIONS = {
    "CAPITAL",
    "DIG",
    "AKRK",
    "GRUPO AKRK",
    "GDC",
    "SEM CESSAO",
    DEFAULT_MISSING_VALUE,
}

# Convênios que devem ser removidos
EXCLUDED_CONVENIOS = {
    "FGTS",
    "CRED TRAB",
}
