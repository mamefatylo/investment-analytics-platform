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

# Jours de bourse par année. Réservé à une série quotidienne.
# Les portefeuilles mensuels annualisent avec MONTHS_PER_YEAR.

ANNUALIZATION_FACTOR = 252

MONTHS_PER_YEAR = 12

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
MARKET = RAW_DIR / "market"
MACRO = RAW_DIR / "macro"
FX = RAW_DIR / "fx"
ODD = RAW_DIR / "odd"

# ---------------------------------------------------------------------------
# PROCESSED DATASETS
# ---------------------------------------------------------------------------

PROCESSED_SECURITIES = PROCESSED_DIR / "securities"
PROCESSED_MARKET = PROCESSED_DIR / "market"
PROCESSED_MACRO = PROCESSED_DIR / "macro"
PROCESSED_ODD = PROCESSED_DIR / "odd"

# ---------------------------------------------------------------------------
# UNIVERSE DATASETS
# ---------------------------------------------------------------------------

CANDIDATE_UNIVERSE = UNIVERSE_DIR / "candidate"
ELIGIBLE_UNIVERSE = UNIVERSE_DIR / "eligible"
EXCLUDED_UNIVERSE = UNIVERSE_DIR / "excluded"

# ---------------------------------------------------------------------------
# PORTFOLIO DATASETS
# ---------------------------------------------------------------------------

PORTFOLIO_DATA = DATA_DIR / "04_portfolios"
MONITORING_DATA = DATA_DIR / "05_monitoring"
PERFORMANCE_DATA = DATA_DIR / "06_performance"

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
MONITORING_REPORTS = REPORTS_DIR / "monitoring"
PERFORMANCE_REPORTS = REPORTS_DIR / "performance"
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

MONITORING_TESTS = (
    TESTS_DIR / "monitoring"
)

PERFORMANCE_TESTS = (
    TESTS_DIR / "performance"
)

RISK_TESTS = (
    TESTS_DIR / "risk"
)

# ============================================================================
# MARKET DATA
# ============================================================================

MARKET_HISTORY_YEARS = 10
# Months of past returns used to set the next quarter's weights.
# Five years is the risk horizon. The month being scored is excluded.
ESTIMATION_MONTHS = 60
# CLIENT001 IPS limits for a long-only equity mandate.
# Issuer 5%, sector 20%, region 50%, listing currency 40%.
# The optimized books keep 25 to 40 names. Gross turnover is at most
# 50% a year, applied as 12.5% at each quarterly rebalance.
ISSUER_WEIGHT_CAP = 0.05
SECTOR_WEIGHT_CAP = 0.20
ZONE_WEIGHT_CAP = 0.50
CURRENCY_WEIGHT_CAP = 0.40
MIN_POSITIONS = 25
MAX_POSITIONS = 40
QUARTERLY_TURNOVER_CAP = 0.125