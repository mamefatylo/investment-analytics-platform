"""
resolver.py

Dynamic acquisition connector resolver.

Purpose
-------
Resolve connector implementations dynamically from the technical
Connector identifier stored in the Source Registry.

Example
-------
Registry value:

    Connector = fred

Resolves module:

    src.acquisition.connectors.fred

No connector registry or provider-specific mapping is embedded
in this module.

Security
--------
Connector identifiers are restricted to safe Python module names.
Arbitrary module paths, relative imports, parent references,
filesystem paths, and dotted module names are rejected.
"""

import importlib
import inspect
import re

from src.acquisition.connectors.base import (
    BaseConnector,
    ConnectorConfigurationError,
    ConnectorDependencyError,
)


# ============================================================================
# TECHNICAL CONTRACT
# ============================================================================

CONNECTOR_PACKAGE = (
    "src.acquisition.connectors"
)

CONNECTOR_NAME_PATTERN = re.compile(
    r"^[a-z][a-z0-9_]*$"
)


# ============================================================================
# CONNECTOR NAME
# ============================================================================

def normalize_connector_name(
    connector,
) -> str:
    """
    Normalize and validate a technical Connector identifier.

    Valid examples
    --------------
    fred
    openfigi
    alpha_vantage
    yfinance

    Invalid examples
    ----------------
    Fred Connector
    ../fred
    src.acquisition.connectors.fred
    fred.py
    _fred
    """

    if connector is None:

        raise ConnectorConfigurationError(
            "Connector is required."
        )

    normalized = str(
        connector
    ).strip().casefold()

    if not normalized:

        raise ConnectorConfigurationError(
            "Connector cannot be empty."
        )

    if not CONNECTOR_NAME_PATTERN.fullmatch(
        normalized
    ):

        raise ConnectorConfigurationError(
            "Connector must be a safe Python "
            "module identifier containing only "
            "lowercase letters, numbers and "
            "underscores, and must begin with "
            "a letter."
        )

    return normalized


# ============================================================================
# MODULE RESOLUTION
# ============================================================================

def build_connector_module_name(
    connector,
) -> str:
    """
    Build the fully qualified connector module name.
    """

    normalized = normalize_connector_name(
        connector
    )

    return (
        f"{CONNECTOR_PACKAGE}."
        f"{normalized}"
    )


def load_connector_module(
    connector,
):
    """
    Import one connector module dynamically.

    Only modules located inside the configured connector package
    can be resolved.
    """

    module_name = (
        build_connector_module_name(
            connector
        )
    )

    try:

        module = importlib.import_module(
            module_name
        )

    except ModuleNotFoundError as error:

        if error.name == module_name:

            raise ConnectorDependencyError(
                "Connector module was not found: "
                f"{module_name}"
            ) from error

        raise ConnectorDependencyError(
            "Connector module dependency could "
            "not be imported while loading "
            f"{module_name}: {error.name}"
        ) from error

    return module


# ============================================================================
# CONNECTOR CLASS DISCOVERY
# ============================================================================

def discover_connector_classes(
    module,
) -> list[type[BaseConnector]]:
    """
    Return concrete BaseConnector implementations defined by a module.

    Imported connector classes from other modules are deliberately
    ignored to keep discovery deterministic.
    """

    candidates = []

    for _, candidate in inspect.getmembers(
        module,
        inspect.isclass,
    ):

        if candidate is BaseConnector:
            continue

        if not issubclass(
            candidate,
            BaseConnector,
        ):
            continue

        if inspect.isabstract(
            candidate
        ):
            continue

        if (
            candidate.__module__
            != module.__name__
        ):
            continue

        candidates.append(
            candidate
        )

    return candidates


def resolve_connector_class(
    connector,
) -> type:
    """
    Resolve exactly one concrete connector implementation.

    Contract
    --------
    Every connector module must define exactly one concrete subclass
    of BaseConnector.
    """

    normalized = normalize_connector_name(
        connector
    )

    module = load_connector_module(
        normalized
    )

    candidates = (
        discover_connector_classes(
            module
        )
    )

    if not candidates:

        raise ConnectorConfigurationError(
            "Connector module "
            f"{module.__name__} does not define "
            "a concrete BaseConnector implementation."
        )

    if len(candidates) > 1:

        candidate_names = sorted(
            candidate.__name__
            for candidate
            in candidates
        )

        raise ConnectorConfigurationError(
            "Connector module "
            f"{module.__name__} defines multiple "
            "concrete BaseConnector implementations: "
            f"{candidate_names}"
        )

    return candidates[0]


# ============================================================================
# CONNECTOR INSTANCE
# ============================================================================

def resolve_connector(
    connector,
) -> BaseConnector:
    """
    Instantiate and return the connector declared by the registry.
    """

    connector_class = (
        resolve_connector_class(
            connector
        )
    )

    try:

        instance = connector_class()

    except Exception as error:

        raise ConnectorConfigurationError(
            "Unable to instantiate Connector "
            f"{connector_class.__name__}."
        ) from error

    if not isinstance(
        instance,
        BaseConnector,
    ):

        raise ConnectorConfigurationError(
            "Resolved connector does not "
            "implement BaseConnector."
        )

    return instance