"""
base.py

Common contracts for acquisition connectors.

Purpose
-------
Define the stable interface between generic acquisition engines
and provider-specific connectors.

Architecture
------------
Source Registry
      |
      v
Acquisition Engine
      |
      v
ConnectorContext
      |
      v
Provider Connector
      |
      v
ConnectorResult
      |
      v
Acquisition Engine
      |
      v
Raw Dataset

Design principles
-----------------
Connectors are responsible only for provider-specific protocol
and technical response handling.

Connectors must not contain:
- business Source IDs;
- business Dataset IDs;
- output file names;
- storage directories;
- database loading logic;
- acquisition logging;
- orchestration logic;
- business filtering;
- business transformations;
- embedded secrets.

Generic acquisition engines remain responsible for:
- Source Registry orchestration;
- Dataset selection;
- input resolution;
- output resolution;
- atomic publication;
- acquisition logging;
- execution-level failure handling.

Provider-specific connectors remain responsible for:
- validating provider-specific Parameters;
- constructing provider-specific requests;
- obtaining authentication material from the environment;
- executing provider-specific communication;
- interpreting provider responses;
- handling provider-specific pagination or batching;
- returning source-faithful, technically normalized tabular data.

Business filtering and analytical transformation belong downstream,
primarily in preparation and quality layers.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


# ============================================================================
# CONNECTOR EXCEPTIONS
# ============================================================================

class ConnectorError(RuntimeError):
    """
    Base exception for all connector failures.

    Acquisition engines may catch this exception when they need
    a common failure boundary around connector execution.
    """


class ConnectorConfigurationError(ConnectorError):
    """
    Raised when connector configuration is missing,
    incomplete, inconsistent, or invalid.
    """


class ConnectorAuthenticationError(ConnectorError):
    """
    Raised when required authentication material is missing
    or rejected by the external provider.
    """


class ConnectorTransportError(ConnectorError):
    """
    Raised when communication with an external service fails.

    Examples include:
    - connection failures;
    - timeouts;
    - exhausted transport retries;
    - HTTP transport failures.
    """


class ConnectorResponseError(ConnectorError):
    """
    Raised when a provider response violates the technical
    contract expected by the connector.
    """


class ConnectorDependencyError(ConnectorError):
    """
    Raised when a connector-specific runtime dependency
    is unavailable or cannot be imported.
    """


# ============================================================================
# CONNECTOR CONTEXT
# ============================================================================

@dataclass(frozen=True)
class ConnectorContext:
    """
    Immutable execution context supplied to a connector.

    Attributes
    ----------
    endpoint
        Optional Access Endpoint resolved from the Source Registry.

        Whether an endpoint is mandatory is determined by the
        Production Method and connector-specific contract.

    parameters
        Provider-specific, non-secret Parameters resolved from the
        Source Registry.

    input_data
        Optional upstream Dataset supplied as a pandas DataFrame.

        This is used when an acquisition requires an existing Dataset
        to construct requests, for example identifier mapping.

    runtime_options
        Optional technical execution settings supplied by the engine.

        These settings are not Dataset business configuration and
        therefore do not belong in Source Registry Parameters.

        Typical examples could include transport timeouts or other
        engine-level execution controls.
    """

    endpoint: str | None

    parameters: dict[str, Any] = field(
        default_factory=dict
    )

    input_data: pd.DataFrame | None = None

    runtime_options: dict[str, Any] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        """
        Validate and defensively isolate the execution context.
        """

        endpoint = self.endpoint

        if endpoint is not None:

            if not isinstance(
                endpoint,
                str,
            ):

                raise TypeError(
                    "ConnectorContext.endpoint must be "
                    "a string or None."
                )

            endpoint = endpoint.strip()

            if not endpoint:

                endpoint = None

        if not isinstance(
            self.parameters,
            dict,
        ):

            raise TypeError(
                "ConnectorContext.parameters must "
                "be a dictionary."
            )

        if (
            self.input_data is not None
            and not isinstance(
                self.input_data,
                pd.DataFrame,
            )
        ):

            raise TypeError(
                "ConnectorContext.input_data must be "
                "a pandas DataFrame or None."
            )

        if not isinstance(
            self.runtime_options,
            dict,
        ):

            raise TypeError(
                "ConnectorContext.runtime_options must "
                "be a dictionary."
            )

        object.__setattr__(
            self,
            "endpoint",
            endpoint,
        )

        object.__setattr__(
            self,
            "parameters",
            self.parameters.copy(),
        )

        object.__setattr__(
            self,
            "runtime_options",
            self.runtime_options.copy(),
        )

        if self.input_data is not None:

            object.__setattr__(
                self,
                "input_data",
                self.input_data.copy(
                    deep=True
                ),
            )


# ============================================================================
# CONNECTOR RESULT
# ============================================================================

@dataclass(frozen=True)
class ConnectorResult:
    """
    Immutable result produced by an acquisition connector.

    Attributes
    ----------
    data
        Source-faithful, technically normalized tabular data returned
        by the connector.

        The connector must not apply portfolio rules, analytical
        filtering, eligibility decisions, or downstream business
        transformations.

    metadata
        Optional non-secret technical execution metadata.

        Examples:
        - provider record count;
        - request count;
        - page count;
        - batch count;
        - response-level technical diagnostics.

        Metadata must never contain authentication secrets.
    """

    data: pd.DataFrame

    metadata: dict[str, Any] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        """
        Validate and defensively isolate connector outputs.
        """

        if not isinstance(
            self.data,
            pd.DataFrame,
        ):

            raise TypeError(
                "ConnectorResult.data must be "
                "a pandas DataFrame."
            )

        if not isinstance(
            self.metadata,
            dict,
        ):

            raise TypeError(
                "ConnectorResult.metadata must be "
                "a dictionary."
            )

        object.__setattr__(
            self,
            "data",
            self.data.copy(
                deep=True
            ),
        )

        object.__setattr__(
            self,
            "metadata",
            self.metadata.copy(),
        )


# ============================================================================
# BASE CONNECTOR
# ============================================================================

class BaseConnector(ABC):
    """
    Abstract interface implemented by acquisition connectors.

    Generic acquisition engines depend only on this interface.

    Engines must never branch on provider names or connector names.
    """
    SUPPORTED_RUNTIME_OPTIONS: frozenset[str] = frozenset()

    @classmethod
    def filter_runtime_options(
        cls,
        runtime_options: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Return runtime options supported by this connector.

        The acquisition engine may expose a common runtime option set
        across all connectors. Each connector declares the subset it
        supports through SUPPORTED_RUNTIME_OPTIONS.

        Unknown engine runtime options are ignored here rather than
        passed to the connector.

        This keeps the acquisition engine provider-agnostic while
        allowing connectors to enforce their own runtime contracts.
        """

        if not isinstance(
            runtime_options,
            dict,
        ):
            raise TypeError(
                "runtime_options must be a dictionary."
            )

        if not isinstance(
            cls.SUPPORTED_RUNTIME_OPTIONS,
            frozenset,
        ):
            raise TypeError(
                "SUPPORTED_RUNTIME_OPTIONS must "
                "be a frozenset."
            )

        return {
            key: value
            for key, value in runtime_options.items()
            if key in cls.SUPPORTED_RUNTIME_OPTIONS
        }

    @classmethod
    @abstractmethod
    def validate_parameters(
        cls,
        parameters: dict[str, Any],
    ) -> None:
        """
        Validate provider-specific Parameters.

        Generic JSON validation has already been performed by
        validate_source_registry.py.

        This method validates provider-specific semantics only.

        Examples
        --------
        A connector may require:
        - an API function;
        - a series identifier;
        - an identifier type;
        - provider-specific options.

        Secrets must not be supplied through Parameters.
        """

        raise NotImplementedError


    @classmethod
    def validate_context(
        cls,
        context: ConnectorContext,
    ) -> None:
        """
        Validate the connector execution context.

        Subclasses may extend this method when the provider requires:
        - an Access Endpoint;
        - upstream input data;
        - specific runtime options.

        Subclasses overriding this method should call:

            super().validate_context(context)
        """

        if not isinstance(
            context,
            ConnectorContext,
        ):

            raise TypeError(
                "context must be a ConnectorContext."
            )

        cls.validate_parameters(
            context.parameters
        )


    @classmethod
    @abstractmethod
    def acquire(
        cls,
        context: ConnectorContext,
    ) -> ConnectorResult:
        """
        Acquire source data and return a normalized connector result.

        The connector should preserve source information as faithfully
        as reasonably possible while converting the response into a
        stable tabular representation.

        The connector must not:
        - resolve Dataset output paths;
        - write final Dataset files;
        - write acquisition logs;
        - load SQLite tables;
        - apply portfolio rules;
        - create analytical datasets;
        - perform downstream business filtering.

        Returns
        -------
        ConnectorResult
            Technically normalized source data plus optional
            non-secret technical metadata.
        """

        raise NotImplementedError