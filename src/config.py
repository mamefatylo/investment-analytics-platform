"""
config.py

Configuration centrale du projet Investment Analytics Platform.

Ce fichier centralise :

- les informations générales du projet ;
- les paramètres globaux ;
- les chemins utilisés dans le projet.

Toute modification de structure ou de chemin doit être réalisée ici.
"""

from pathlib import Path

# ============================================================================
# PROJECT INFORMATION
# ============================================================================

PROJECT_NAME = "Investment Analytics Platform"

VERSION = "2.0"

REPORTING_CURRENCY = "CHF"

# ============================================================================
# ANALYSIS PARAMETERS
# ============================================================================

# Définis ultérieurement après Data Quality
# et la construction de l'univers investissable.

START_DATE = None
END_DATE = None

# Nombre conventionnel de jours de bourse par année

ANNUALIZATION_FACTOR = 252

# ============================================================================
# PROJECT ROOT
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ============================================================================
# DOCUMENTATION
# ============================================================================

DOCUMENTATION_DIR = PROJECT_ROOT / "documentation"

# ============================================================================
# DATA
# ============================================================================

DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "01_raw"
PROCESSED_DIR = DATA_DIR / "02_processed"
UNIVERSE_DIR = DATA_DIR / "03_universe"

# ---------------------------------------------------------------------------
# RAW DATASETS
# ---------------------------------------------------------------------------

BENCHMARK = RAW_DIR / "benchmark"
SECURITIES = RAW_DIR / "securities"
MARKET = RAW_DIR / "market"
FUNDAMENTALS = RAW_DIR / "fundamentals"
MACRO = RAW_DIR / "macro"
FX = RAW_DIR / "fx"
ODD = RAW_DIR / "odd"
EXCLUSIONS = RAW_DIR / "exclusions"

# ---------------------------------------------------------------------------
# PROCESSED DATASETS
# ---------------------------------------------------------------------------

PROCESSED_SECURITIES = PROCESSED_DIR / "securities"
PROCESSED_MARKET = PROCESSED_DIR / "market"
PROCESSED_FUNDAMENTALS = PROCESSED_DIR / "fundamentals"
PROCESSED_MACRO = PROCESSED_DIR / "macro"
PROCESSED_FX = PROCESSED_DIR / "fx"
PROCESSED_ODD = PROCESSED_DIR / "odd"
PROCESSED_EXCLUSIONS = PROCESSED_DIR / "exclusions"

# ---------------------------------------------------------------------------
# UNIVERSE DATASETS
# ---------------------------------------------------------------------------

CANDIDATE_UNIVERSE = UNIVERSE_DIR / "candidate"
ELIGIBLE_UNIVERSE = UNIVERSE_DIR / "eligible"
EXCLUDED_UNIVERSE = UNIVERSE_DIR / "excluded"

# ============================================================================
# DATABASE
# ============================================================================

DATABASE_DIR = PROJECT_ROOT / "database"

DATABASE_FILE = DATABASE_DIR / "sdg_investment.db"

# ============================================================================
# SOURCE CODE
# ============================================================================

SRC_DIR = PROJECT_ROOT / "src"

PREPARATION = SRC_DIR / "preparation"
ACQUISITION = SRC_DIR / "acquisition"
QUALITY = SRC_DIR / "quality"

SECURITIES_MASTER_MODULE = (
    SRC_DIR / "securities_master"
)

UNIVERSE_MODULE = (
    SRC_DIR / "universe"
)

PORTFOLIO_CONSTRUCTION = (
    SRC_DIR / "portfolio_construction"
)

PORTFOLIO_MONITORING = (
    SRC_DIR / "portfolio_monitoring"
)

PERFORMANCE = SRC_DIR / "performance"

RISK = SRC_DIR / "risk"

STRESS_TESTING = (
    SRC_DIR / "stress_testing"
)

REPORTING = SRC_DIR / "reporting"

# ============================================================================
# DASHBOARDS
# ============================================================================

DASHBOARDS_DIR = PROJECT_ROOT / "dashboards"

POWERBI = DASHBOARDS_DIR / "powerbi"

# ============================================================================
# REPORTS
# ============================================================================

REPORTS_DIR = PROJECT_ROOT / "reports"

PORTFOLIO_REPORTS = REPORTS_DIR / "portfolio"
RISK_REPORTS = REPORTS_DIR / "risk"

MANAGEMENT_REPORTS = (
    REPORTS_DIR / "management"
)

EXECUTIVE_REPORTS = (
    REPORTS_DIR / "executive"
)

# ============================================================================
# AUDIT & GOVERNANCE
# ============================================================================

AUDIT_DIR = PROJECT_ROOT / "audit"

SOURCE_REGISTRY_FILE = (
    AUDIT_DIR / "source_registry.xlsx"
)

ACQUISITION_LOG_FILE = (
    AUDIT_DIR / "acquisition_log.xlsx"
)

DATA_CONTROL_CENTER_FILE = (
    AUDIT_DIR / "data_control_center.xlsx"
)

# ============================================================================
# LOGGING
# ============================================================================

LOGS_DIR = PROJECT_ROOT / "logs"

TECHNICAL_LOG = LOGS_DIR / "technical.log"

# ============================================================================
# TESTS
# ============================================================================

TESTS_DIR = PROJECT_ROOT / "tests"

ACQUISITION_TESTS = (
    TESTS_DIR / "acquisition"
)

QUALITY_TESTS = (
    TESTS_DIR / "quality"
)

PORTFOLIO_TESTS = (
    TESTS_DIR / "portfolio"
)

RISK_TESTS = (
    TESTS_DIR / "risk"
)