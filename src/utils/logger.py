"""
logger.py

Configuration centralisée du logging.
"""

import logging
from logging.handlers import RotatingFileHandler

from src.config import (
    TECHNICAL_LOG,
)

# ============================================================================
# LOG DIRECTORY
# ============================================================================

TECHNICAL_LOG.parent.mkdir(
    parents=True,
    exist_ok=True,
)

# ============================================================================
# LOGGER
# ============================================================================

# Le logger centralise les événements techniques produits par
# les scripts de la plateforme.
#
# Les messages sont enregistrés dans le fichier :
#
# logs/technical.log
#
# Contrairement à l'Acquisition Log (audit/acquisition_log.xlsx),
# qui conserve la traçabilité métier des datasets acquis,
# le Technical Log documente l'exécution des traitements,
# les validations réalisées, les avertissements et les erreurs.
#
# Un RotatingFileHandler est utilisé afin de limiter la taille
# du fichier de log et de conserver plusieurs historiques
# d'exécution.

logger = logging.getLogger(
    "investment_platform"
)

logger.setLevel(logging.INFO)

logger.propagate = False

if not logger.handlers:

    handler = RotatingFileHandler(
        TECHNICAL_LOG,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )

    formatter = logging.Formatter(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )

    handler.setFormatter(
        formatter
    )

    logger.addHandler(
        handler
    )