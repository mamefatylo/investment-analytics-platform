"""
Configuration du framework qualité.

Les six contrôles correspondent au brief de la phase 4.
Un contrôle non applicable est exclu du score : il ne reçoit pas
une note de 100 par convention.
"""

PASS_RATE = 95.0
FAIL_RATE = 80.0

CONTROL_ORDER = (
    "Valeurs manquantes",
    "Doublons",
    "Valeurs aberrantes",
    "Formats incorrects",
    "Cohérence des devises",
    "Chronologie",
)

CONTROL_IDS = {
    "Valeurs manquantes": "QC-001",
    "Doublons": "QC-002",
    "Valeurs aberrantes": "QC-003",
    "Formats incorrects": "QC-004",
    "Cohérence des devises": "QC-005",
    "Chronologie": "QC-006",
}

GRADE_THRESHOLDS = {
    "A": 97.0,
    "B": 90.0,
    "C": 80.0,
    "D": 0.0,
}

STATUS_RANK = {
    "PASS": 0,
    "NOT_APPLICABLE": 0,
    "WARNING": 1,
    "FAIL": 2,
}

ISO_CURRENCY_PATTERN = r"^[A-Z]{3}$"
FIGI_PATTERN = r"^[A-Z0-9]{12}$"
BLANK_TOKENS = {"", "nan", "none", "nat", "."}
OHLC_RELATIVE_TOLERANCE = 0.001
OHLC_ABSOLUTE_TOLERANCE = 0.01
