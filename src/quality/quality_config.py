"""
Configuration centralisée du framework qualité.
"""

PASS_THRESHOLD = 95.0

WARNING_THRESHOLD = 80.0


QUALITY_WEIGHTS = {
    "Completeness Rate": 0.35,
    "Uniqueness Rate": 0.20,
    "Availability Rate": 0.20,
    "Freshness Rate": 0.15,
    "Chronology Rate": 0.10,
}


GRADE_THRESHOLDS = {
    "A": 97.0,
    "B": 90.0,
    "C": 80.0,
    "D": 0.0,
}


OPTIONAL_COLUMNS = {
    "reason",
}