"""
fred.py

Source-faithful FRED observations connector.

Purpose
-------
Acquire observations from the FRED series observations API without
applying downstream analytical transformations.

Responsibilities
----------------
The connector is responsible for:
- validating FRED-specific Parameters;
- loading FRED authentication material from the environment;
- constructing FRED observation requests;
- acquiring one or multiple configured series;
- handling provider pagination;
- interpreting FRED JSON responses;
- preserving observation-level source fields;
- returning technically normalized tabular data;
- returning non-secret execution metadata.

The connector does not:
- read the Source Registry;
- contain business Source IDs;
- contain business Dataset IDs;
- contain the FRED endpoint;
- contain output file names;
- contain storage directories;
- resolve output paths;
- publish files;
- write acquisition logs;
- load SQLite tables;
- calculate returns;
- resample observations;
- transform units implicitly;
- fill missing observations;
- perform analytical filtering.

Business interpretation belongs downstream in preparation
and quality layers.
"""

import os
import time
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from src.acquisition.connectors.base import (
    BaseConnector,
    ConnectorAuthenticationError,
    ConnectorConfigurationError,
    ConnectorContext,
    ConnectorResponseError,
    ConnectorResult,
    ConnectorTransportError,
)
from src.utils.logger import logger


# ============================================================================
# AUTHENTICATION CONTRACT
# ============================================================================

API_KEY_ENVIRONMENT_VARIABLE = "FRED_API_KEY"


# ============================================================================
# FRED DATASET PARAMETER CONTRACT
# ============================================================================

SERIES_ID_PARAMETER = "series_id"
SERIES_IDS_PARAMETER = "series_ids"
FILE_TYPE_PARAMETER = "file_type"

REQUIRED_FILE_TYPE = "json"

SERIES_PARAMETERS = {
    SERIES_ID_PARAMETER,
    SERIES_IDS_PARAMETER,
}

SUPPORTED_REQUEST_PARAMETERS = {
    "realtime_start",
    "realtime_end",
    "limit",
    "offset",
    "sort_order",
    "observation_start",
    "observation_end",
    "units",
    "frequency",
    "aggregation_method",
    "output_type",
    "vintage_dates",
}

SUPPORTED_DATASET_PARAMETERS = (
    SERIES_PARAMETERS
    | SUPPORTED_REQUEST_PARAMETERS
    | {
        FILE_TYPE_PARAMETER,
    }
)


# ============================================================================
# PROVIDER CONTRACT
# ============================================================================

FRED_MAX_RESULTS_PER_REQUEST = 100000

RETRYABLE_STATUS_CODES = frozenset(
    {
        408,
        429,
        500,
        502,
        503,
        504,
    }
)


# ============================================================================
# RUNTIME DEFAULTS
# ============================================================================

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_READ_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_USER_AGENT = "investment-data-pipeline/1.0"


# ============================================================================
# NORMALIZATION
# ============================================================================

def optional_text(
    value: Any,
) -> str | None:
    """Normalize optional scalar text."""

    if value is None:
        return None

    if not isinstance(
        value,
        (
            dict,
            list,
            tuple,
            set,
        ),
    ):
        try:
            if pd.isna(value):
                return None
        except (
            TypeError,
            ValueError,
        ):
            pass

    normalized = str(value).strip()

    return normalized or None


def positive_integer(
    value: Any,
    field_name: str,
) -> int:
    """Normalize a strictly positive integer."""

    if isinstance(
        value,
        bool,
    ):
        raise ConnectorConfigurationError(
            f"{field_name} must be "
            "a positive integer."
        )

    try:
        normalized = int(value)
    except (
        TypeError,
        ValueError,
    ) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be "
            "a positive integer."
        ) from error

    if normalized <= 0:
        raise ConnectorConfigurationError(
            f"{field_name} must be "
            "greater than zero."
        )

    return normalized


def non_negative_integer(
    value: Any,
    field_name: str,
) -> int:
    """Normalize a non-negative integer."""

    if isinstance(
        value,
        bool,
    ):
        raise ConnectorConfigurationError(
            f"{field_name} must be "
            "a non-negative integer."
        )

    try:
        normalized = int(value)
    except (
        TypeError,
        ValueError,
    ) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be "
            "a non-negative integer."
        ) from error

    if normalized < 0:
        raise ConnectorConfigurationError(
            f"{field_name} cannot be negative."
        )

    return normalized


def positive_number(
    value: Any,
    field_name: str,
) -> float:
    """Normalize a strictly positive numeric value."""

    if isinstance(
        value,
        bool,
    ):
        raise ConnectorConfigurationError(
            f"{field_name} must be positive."
        )

    try:
        normalized = float(value)
    except (
        TypeError,
        ValueError,
    ) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be numeric."
        ) from error

    if normalized <= 0:
        raise ConnectorConfigurationError(
            f"{field_name} must be "
            "greater than zero."
        )

    return normalized


# ============================================================================
# AUTHENTICATION
# ============================================================================

def load_api_key() -> str:
    """
    Load the FRED API key from the environment.

    Secrets are never obtained from Dataset Parameters.
    """

    load_dotenv()

    api_key = optional_text(
        os.getenv(
            API_KEY_ENVIRONMENT_VARIABLE
        )
    )

    if api_key is None:
        raise ConnectorAuthenticationError(
            f"{API_KEY_ENVIRONMENT_VARIABLE} "
            "is missing from the environment."
        )

    return api_key


# ============================================================================
# SERIES CONFIGURATION
# ============================================================================

def normalize_series_ids(
    parameters: dict[str, Any],
) -> list:
    """
    Resolve configured FRED series identifiers.

    Exactly one of series_id or series_ids must be supplied.
    """

    single_series = parameters.get(
        SERIES_ID_PARAMETER
    )

    multiple_series = parameters.get(
        SERIES_IDS_PARAMETER
    )

    normalized_single = optional_text(
        single_series
    )

    has_single = (
        normalized_single is not None
    )

    has_multiple = (
        multiple_series is not None
    )

    if (
        has_single
        and has_multiple
    ):
        raise ConnectorConfigurationError(
            "FRED Parameters must use either "
            "series_id or series_ids, not both."
        )

    if (
        not has_single
        and not has_multiple
    ):
        raise ConnectorConfigurationError(
            "FRED Parameters require "
            "series_id or series_ids."
        )

    if has_single:
        return [
            normalized_single
        ]

    if not isinstance(
        multiple_series,
        list,
    ):
        raise ConnectorConfigurationError(
            "FRED series_ids must be "
            "a JSON array."
        )

    if not multiple_series:
        raise ConnectorConfigurationError(
            "FRED series_ids cannot be empty."
        )

    normalized = []

    for value in multiple_series:

        series_id = optional_text(
            value
        )

        if series_id is None:
            raise ConnectorConfigurationError(
                "FRED series_ids cannot contain "
                "empty values."
            )

        normalized.append(
            series_id
        )

    normalized_keys = [
        series_id.casefold()
        for series_id in normalized
    ]

    if (
        len(normalized_keys)
        != len(set(normalized_keys))
    ):
        raise ConnectorConfigurationError(
            "FRED series_ids contains "
            "duplicate identifiers."
        )

    return normalized


# ============================================================================
# DATASET REQUEST PARAMETERS
# ============================================================================

def build_request_parameters(
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """
    Build request parameters shared across configured series.

    series_id / series_ids are handled separately.
    api_key is injected from the environment.
    file_type is forced to JSON for deterministic parsing.
    """

    request_parameters = {
        key: value
        for key, value in parameters.items()
        if key in SUPPORTED_REQUEST_PARAMETERS
    }

    if "limit" in request_parameters:

        limit = positive_integer(
            request_parameters[
                "limit"
            ],
            "limit",
        )

        if (
            limit
            > FRED_MAX_RESULTS_PER_REQUEST
        ):
            raise ConnectorConfigurationError(
                "FRED limit cannot exceed "
                f"{FRED_MAX_RESULTS_PER_REQUEST}."
            )

        request_parameters[
            "limit"
        ] = limit

    if "offset" in request_parameters:

        request_parameters[
            "offset"
        ] = non_negative_integer(
            request_parameters[
                "offset"
            ],
            "offset",
        )

    return request_parameters


# ============================================================================
# RUNTIME OPTIONS
# ============================================================================

def resolve_runtime_options(
    runtime_options: dict[str, Any],
) -> dict[str, Any]:
    """
    Resolve technical execution options provided by the engine.
    """

    if not isinstance(
        runtime_options,
        dict,
    ):
        raise ConnectorConfigurationError(
            "runtime_options must be "
            "a dictionary."
        )

    allowed_options = {
        "connect_timeout",
        "read_timeout",
        "max_attempts",
        "backoff_seconds",
        "user_agent",
    }

    unsupported = sorted(
        set(runtime_options)
        - allowed_options
    )

    if unsupported:
        raise ConnectorConfigurationError(
            "Unsupported FRED runtime options: "
            f"{unsupported}"
        )

    connect_timeout = positive_number(
        runtime_options.get(
            "connect_timeout",
            DEFAULT_CONNECT_TIMEOUT_SECONDS,
        ),
        "connect_timeout",
    )

    read_timeout = positive_number(
        runtime_options.get(
            "read_timeout",
            DEFAULT_READ_TIMEOUT_SECONDS,
        ),
        "read_timeout",
    )

    max_attempts = positive_integer(
        runtime_options.get(
            "max_attempts",
            DEFAULT_MAX_ATTEMPTS,
        ),
        "max_attempts",
    )

    backoff_seconds = positive_number(
        runtime_options.get(
            "backoff_seconds",
            DEFAULT_BACKOFF_SECONDS,
        ),
        "backoff_seconds",
    )

    user_agent = optional_text(
        runtime_options.get(
            "user_agent",
            DEFAULT_USER_AGENT,
        )
    )

    if user_agent is None:
        raise ConnectorConfigurationError(
            "user_agent cannot be empty."
        )

    return {
        "connect_timeout":
            connect_timeout,
        "read_timeout":
            read_timeout,
        "max_attempts":
            max_attempts,
        "backoff_seconds":
            backoff_seconds,
        "user_agent":
            user_agent,
    }

# ============================================================================
# HTTP / RETRY
# ============================================================================

def resolve_retry_delay(
    response: requests.Response,
    *,
    attempt: int,
    backoff_seconds: float,
) -> float:
    """
    Resolve Retry-After or exponential backoff delay.

    Retry-After is preferred when the provider supplies it.
    Otherwise exponential backoff is applied.
    """

    retry_after = optional_text(
        response.headers.get(
            "Retry-After"
        )
    )

    if retry_after is not None:

        try:

            return max(
                float(
                    retry_after
                ),
                0.0,
            )

        except ValueError:

            pass

    return (
        backoff_seconds
        * (
            2 ** (
                attempt - 1
            )
        )
    )


def request_json(
    *,
    session: requests.Session,
    endpoint: str,
    query: dict[str, Any],
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    backoff_seconds: float,
) -> dict[str, Any]:
    """
    Execute one FRED JSON request with bounded retries.

    Transport failures and retryable HTTP responses are retried.
    Authentication and non-retryable HTTP failures fail immediately.
    """

    if not isinstance(
        session,
        requests.Session,
    ):

        raise TypeError(
            "session must be a requests.Session."
        )

    normalized_endpoint = optional_text(
        endpoint
    )

    if normalized_endpoint is None:

        raise ConnectorConfigurationError(
            "FRED endpoint cannot be empty."
        )

    if not isinstance(
        query,
        dict,
    ):

        raise TypeError(
            "query must be a dictionary."
        )

    timeout = (
        connect_timeout,
        read_timeout,
    )

    for attempt in range(
        1,
        max_attempts + 1,
    ):

        try:

            response = session.get(
                normalized_endpoint,
                params=query,
                timeout=timeout,
            )

        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:

            if attempt == max_attempts:

                raise ConnectorTransportError(
                    "FRED transport request failed "
                    "after all configured attempts."
                ) from error

            wait_seconds = (
                backoff_seconds
                * (
                    2 ** (
                        attempt - 1
                    )
                )
            )

            logger.warning(
                "FRED transport retry "
                "%s/%s in %.2f seconds",
                attempt,
                max_attempts,
                wait_seconds,
            )

            time.sleep(
                wait_seconds
            )

            continue

        status_code = (
            response.status_code
        )

        if (
            status_code
            in RETRYABLE_STATUS_CODES
        ):

            if attempt == max_attempts:

                raise ConnectorTransportError(
                    "FRED returned retryable "
                    "HTTP status after all "
                    "configured attempts: "
                    f"{status_code}"
                )

            wait_seconds = (
                resolve_retry_delay(
                    response,
                    attempt=attempt,
                    backoff_seconds=(
                        backoff_seconds
                    ),
                )
            )

            logger.warning(
                "FRED HTTP retry "
                "%s/%s after status %s "
                "in %.2f seconds",
                attempt,
                max_attempts,
                status_code,
                wait_seconds,
            )

            time.sleep(
                wait_seconds
            )

            continue

        try:

            response.raise_for_status()

        except requests.HTTPError as error:

            if status_code in {
                401,
                403,
            }:

                raise ConnectorAuthenticationError(
                    "FRED authentication "
                    "was rejected."
                ) from error

            response_message = None

            try:

                error_payload = (
                    response.json()
                )

                if isinstance(
                    error_payload,
                    dict,
                ):

                    response_message = (
                        optional_text(
                            error_payload.get(
                                "error_message"
                            )
                        )
                        or optional_text(
                            error_payload.get(
                                "message"
                            )
                        )
                    )

            except (
                ValueError,
                requests.JSONDecodeError,
            ):

                response_message = None

            if response_message is None:

                response_message = (
                    optional_text(
                        response.text
                    )
                )

            detail = (
                f" Provider response: "
                f"{response_message}"
                if response_message
                else ""
            )

            raise ConnectorTransportError(
                "FRED request failed with "
                f"HTTP {status_code}."
                f"{detail}"
            ) from error

        try:

            payload = response.json()

        except (
            ValueError,
            requests.JSONDecodeError,
        ) as error:

            raise ConnectorResponseError(
                "FRED response is not "
                "valid JSON."
            ) from error

        if not isinstance(
            payload,
            dict,
        ):

            raise ConnectorResponseError(
                "FRED response must be "
                "a JSON object."
            )

        provider_error = (
            optional_text(
                payload.get(
                    "error_message"
                )
            )
            or optional_text(
                payload.get(
                    "message"
                )
            )
        )

        if provider_error is not None:

            raise ConnectorResponseError(
                "FRED returned an API error: "
                f"{provider_error}"
            )

        return payload

    raise ConnectorTransportError(
        "FRED request exhausted all "
        "configured attempts."
    )


# ============================================================================
# RESPONSE CONTRACT
# ============================================================================

def parse_pagination_integer(
    payload: dict[str, Any],
    field_name: str,
) -> int:
    """
    Parse a non-negative integer from FRED pagination metadata.
    """

    if field_name not in payload:

        raise ConnectorResponseError(
            "FRED response is missing "
            f"pagination field: {field_name}"
        )

    value = payload[
        field_name
    ]

    if isinstance(
        value,
        bool,
    ):

        raise ConnectorResponseError(
            "FRED pagination field "
            f"{field_name} is invalid."
        )

    try:

        normalized = int(
            value
        )

    except (
        TypeError,
        ValueError,
    ) as error:

        raise ConnectorResponseError(
            "FRED pagination field "
            f"{field_name} is not an integer."
        ) from error

    if normalized < 0:

        raise ConnectorResponseError(
            "FRED pagination field "
            f"{field_name} cannot be negative."
        )

    return normalized


def validate_response_contract(
    payload: dict[str, Any],
) -> None:
    """
    Validate the minimum FRED observations response contract.
    """

    if not isinstance(
        payload,
        dict,
    ):

        raise ConnectorResponseError(
            "FRED response must be "
            "a JSON object."
        )

    observations = payload.get(
        "observations"
    )

    if not isinstance(
        observations,
        list,
    ):

        raise ConnectorResponseError(
            "FRED response does not contain "
            "an observations list."
        )

    count = parse_pagination_integer(
        payload,
        "count",
    )

    offset = parse_pagination_integer(
        payload,
        "offset",
    )

    limit = parse_agination_integer(
        payload,
        "limit",
    )

    if limit <= 0:

        raise ConnectorResponseError(
            "FRED response limit must "
            "be greater than zero."
        )

    if offset > count and count > 0:

        raise ConnectorResponseError(
            "FRED response offset exceeds "
            "the reported observation count."
        )


# ============================================================================
# RAW OBSERVATION NORMALIZATION
# ============================================================================

def normalize_observations(
    *,
    series_id: str,
    payload: dict[str, Any],
    request_offset: int,
) -> pd.DataFrame:
    """
    Normalize one FRED observations response.

    Acquisition remains RAW-first:
    - value is not converted to numeric;
    - dates are not converted to pandas datetime;
    - missing values are not imputed;
    - frequency is not changed;
    - units are not transformed;
    - observations are not filtered.

    Technical lineage fields are added to preserve series identity
    and deterministic observation ordering.
    """

    validate_response_contract(
        payload
    )

    observations = payload[
        "observations"
    ]

    records = []

    for page_position, observation in enumerate(
        observations
    ):

        if not isinstance(
            observation,
            dict,
        ):

            raise ConnectorResponseError(
                "FRED observation must be "
                "a JSON object."
            )

        record = {
            "series_id":
                series_id,
            "observation_position":
                (
                    request_offset
                    + page_position
                ),
        }

        record.update(
            observation
        )

        records.append(
            record
        )

    if records:

        return (
            pd.DataFrame(
                records
            )
            .reset_index(
                drop=True
            )
        )

    return pd.DataFrame(
        columns=[
            "series_id",
            "observation_position",
            "realtime_start",
            "realtime_end",
            "date",
            "value",
        ]
    )


# ============================================================================
# SERIES PAGINATION
# ============================================================================

def acquire_series(
    *,
    session: requests.Session,
    endpoint: str,
    api_key: str,
    series_id: str,
    request_parameters: dict[str, Any],
    runtime: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Acquire the configured observation range for one FRED series.

    Pagination follows the provider response using:
    - count;
    - offset;
    - limit.

    The connector continues until the requested response space
    has been exhausted.
    """

    normalized_series_id = optional_text(
        series_id
    )

    if normalized_series_id is None:

        raise ConnectorConfigurationError(
            "FRED series_id cannot be empty."
        )

    if not isinstance(
        request_parameters,
        dict,
    ):

        raise TypeError(
            "request_parameters must be "
            "a dictionary."
        )

    if not isinstance(
        runtime,
        dict,
    ):

        raise TypeError(
            "runtime must be a dictionary."
        )

    configured_offset = (
        request_parameters.get(
            "offset",
            0,
        )
    )

    configured_limit = (
        request_parameters.get(
            "limit",
            FRED_MAX_RESULTS_PER_REQUEST,
        )
    )

    initial_offset = (
        non_negative_integer(
            configured_offset,
            "offset",
        )
    )

    page_limit = positive_integer(
        configured_limit,
        "limit",
    )

    if (
        page_limit
        > FRED_MAX_RESULTS_PER_REQUEST
    ):

        raise ConnectorConfigurationError(
            "FRED limit cannot exceed "
            f"{FRED_MAX_RESULTS_PER_REQUEST}."
        )

    offset = (
        initial_offset
    )

    frames = []

    request_count = 0
    observation_count = 0
    reported_total = None

    while True:

        query = dict(
            request_parameters
        )

        query.update(
            {
                "series_id":
                    normalized_series_id,
                "api_key":
                    api_key,
                "file_type":
                    REQUIRED_FILE_TYPE,
                "offset":
                    offset,
                "limit":
                    page_limit,
            }
        )

        payload = request_json(
            session=session,
            endpoint=endpoint,
            query=query,
            connect_timeout=(
                runtime[
                    "connect_timeout"
                ]
            ),
            read_timeout=(
                runtime[
                    "read_timeout"
                ]
            ),
            max_attempts=(
                runtime[
                    "max_attempts"
                ]
            ),
            backoff_seconds=(
                runtime[
                    "backoff_seconds"
                ]
            ),
        )

        request_count += 1

        validate_response_contract(
            payload
        )

        response_count = (
            parse_pagination_integer(
                payload,
                "count",
            )
        )

        response_offset = (
            parse_pagination_integer(
                payload,
                "offset",
            )
        )

        response_limit = (
            parse_pagination_integer(
                payload,
                "limit",
            )
        )

        if (
            response_limit
            > FRED_MAX_RESULTS_PER_REQUEST
        ):

            raise ConnectorResponseError(
                "FRED response limit exceeds "
                "the supported provider maximum."
            )

        if reported_total is None:

            reported_total = (
                response_count
            )

        elif (
            response_count
            != reported_total
        ):

            logger.warning(
                "FRED reported observation count "
                "changed during pagination for "
                "series %s: %s -> %s",
                normalized_series_id,
                reported_total,
                response_count,
            )

            reported_total = (
                response_count
            )

        if (
            response_offset
            != offset
        ):

            raise ConnectorResponseError(
                "FRED response offset does not "
                "match the requested offset. "
                f"Requested={offset}; "
                f"Received={response_offset}"
            )

        frame = normalize_observations(
            series_id=(
                normalized_series_id
            ),
            payload=payload,
            request_offset=(
                response_offset
            ),
        )

        frames.append(
            frame
        )

        received = len(
            frame
        )

        observation_count += (
            received
        )

        next_offset = (
            response_offset
            + received
        )

        if received == 0:
            break

        if next_offset >= response_count:
            break

        if received < response_limit:
            break

        if next_offset <= offset:

            raise ConnectorResponseError(
                "FRED pagination failed "
                "to advance."
            )

        offset = (
            next_offset
        )

    if frames:

        dataframe = pd.concat(
            frames,
            ignore_index=True,
        )

    else:

        dataframe = pd.DataFrame(
            columns=[
                "series_id",
                "observation_position",
                "realtime_start",
                "realtime_end",
                "date",
                "value",
            ]
        )

    duplicate_positions = (
        dataframe.duplicated(
            subset=[
                "series_id",
                "observation_position",
            ],
            keep=False,
        )
        if not dataframe.empty
        else pd.Series(
            dtype=bool
        )
    )

    if (
        not dataframe.empty
        and duplicate_positions.any()
    ):

        raise ConnectorResponseError(
            "FRED pagination produced "
            "duplicate observation positions "
            f"for series {normalized_series_id}."
        )

    metadata = {
        "http_requests":
            request_count,
        "observations":
            observation_count,
        "reported_count":
            (
                int(reported_total)
                if reported_total is not None
                else 0
            ),
        "initial_offset":
            initial_offset,
    }

    return (
        dataframe,
        metadata,
    )


# ============================================================================
# CONNECTOR
# ============================================================================

class FredConnector(BaseConnector):
    """
    Source-faithful FRED observations connector.

    The connector retrieves all observations requested by the
    Dataset Parameters and performs technical normalization only.

    No analytical transformation, interpolation, resampling,
    filtering, or business logic is applied.
    """

    SUPPORTED_RUNTIME_OPTIONS = frozenset(
        {
            "connect_timeout",
            "read_timeout",
            "max_attempts",
            "backoff_seconds",
            "user_agent",
        }
    )

    
    @classmethod
    def validate_parameters(
        cls,
        parameters: dict[str, Any],
    ) -> None:
        """
        Validate FRED-specific Dataset Parameters.

        Exactly one of:
        - series_id
        - series_ids

        must be supplied.

        file_type may be omitted or explicitly configured as json.
        The connector uses JSON internally for deterministic parsing.
        """

        if not isinstance(
            parameters,
            dict,
        ):

            raise ConnectorConfigurationError(
                "FRED Parameters must "
                "be a dictionary."
            )

        unsupported_parameters = sorted(
            set(
                parameters
            )
            - SUPPORTED_DATASET_PARAMETERS
        )

        if unsupported_parameters:

            raise ConnectorConfigurationError(
                "Unsupported FRED Dataset "
                "Parameters: "
                f"{unsupported_parameters}"
            )

        normalize_series_ids(
            parameters
        )

        configured_file_type = optional_text(
            parameters.get(
                FILE_TYPE_PARAMETER
            )
        )

        if (
            configured_file_type is not None
            and configured_file_type.casefold()
            != REQUIRED_FILE_TYPE
        ):

            raise ConnectorConfigurationError(
                "FRED connector requires "
                "file_type=json."
            )

        build_request_parameters(
            parameters
        )


    @classmethod
    def validate_context(
        cls,
        context: ConnectorContext,
    ) -> None:
        """
        Validate the complete FRED execution context.

        FRED observations acquisition requires:
        - an Access Endpoint;
        - valid FRED Dataset Parameters;
        - no upstream input Dataset.

        Technical runtime options are validated separately
        from Dataset Parameters.
        """

        super().validate_context(
            context
        )

        endpoint = optional_text(
            context.endpoint
        )

        if endpoint is None:

            raise ConnectorConfigurationError(
                "FRED requires Access Endpoint."
            )

        if context.input_data is not None:

            raise ConnectorConfigurationError(
                "FRED observations acquisition "
                "does not require input_data."
            )

        resolve_runtime_options(
            context.runtime_options
        )


    @classmethod
    def acquire(
        cls,
        context: ConnectorContext,
    ) -> ConnectorResult:
        """
        Acquire all configured FRED observations.

        Each configured series is acquired independently and
        concatenated into one source-faithful tabular Dataset.

        Returns
        -------
        ConnectorResult
            data
                Raw-first FRED observations.

            metadata
                Non-secret technical execution metrics.
        """

        cls.validate_context(
            context
        )

        endpoint = optional_text(
            context.endpoint
        )

        if endpoint is None:

            raise ConnectorConfigurationError(
                "FRED requires Access Endpoint."
            )

        parameters = (
            context.parameters
            .copy()
        )

        series_ids = normalize_series_ids(
            parameters
        )

        request_parameters = (
            build_request_parameters(
                parameters
            )
        )

        runtime = resolve_runtime_options(
            context.runtime_options
        )

        api_key = load_api_key()

        frames = []

        total_requests = 0
        total_observations = 0

        series_metadata = {}

        with requests.Session() as session:

            session.headers.update(
                {
                    "Accept":
                        "application/json",
                    "User-Agent":
                        runtime[
                            "user_agent"
                        ],
                }
            )

            for series_id in series_ids:

                logger.info(
                    "FRED series acquisition "
                    "started: %s",
                    series_id,
                )

                (
                    frame,
                    metadata,
                ) = acquire_series(
                    session=session,
                    endpoint=endpoint,
                    api_key=api_key,
                    series_id=series_id,
                    request_parameters=(
                        request_parameters
                    ),
                    runtime=runtime,
                )

                frames.append(
                    frame
                )

                total_requests += int(
                    metadata[
                        "http_requests"
                    ]
                )

                total_observations += int(
                    metadata[
                        "observations"
                    ]
                )

                series_metadata[
                    series_id
                ] = {
                    "http_requests":
                        int(
                            metadata[
                                "http_requests"
                            ]
                        ),
                    "observations":
                        int(
                            metadata[
                                "observations"
                            ]
                        ),
                    "reported_count":
                        int(
                            metadata[
                                "reported_count"
                            ]
                        ),
                    "initial_offset":
                        int(
                            metadata[
                                "initial_offset"
                            ]
                        ),
                }

                logger.info(
                    "FRED series acquisition "
                    "completed: %s; "
                    "Requests=%s; "
                    "Observations=%s",
                    series_id,
                    metadata[
                        "http_requests"
                    ],
                    metadata[
                        "observations"
                    ],
                )

        if frames:

            dataframe = pd.concat(
                frames,
                ignore_index=True,
                sort=False,
            )

        else:

            dataframe = pd.DataFrame(
                columns=[
                    "series_id",
                    "observation_position",
                    "realtime_start",
                    "realtime_end",
                    "date",
                    "value",
                ]
            )

        # --------------------------------------------------------------------
        # TECHNICAL RESULT CONSISTENCY
        # --------------------------------------------------------------------

        if (
            len(
                dataframe
            )
            != total_observations
        ):

            raise ConnectorResponseError(
                "FRED normalized record count "
                "does not match accumulated "
                "observation count. "
                f"DataFrame={len(dataframe)}; "
                f"Accumulated={total_observations}"
            )

        if not dataframe.empty:

            if (
                "series_id"
                not in dataframe.columns
            ):

                raise ConnectorResponseError(
                    "FRED normalized output "
                    "is missing series_id."
                )

            if (
                "observation_position"
                not in dataframe.columns
            ):

                raise ConnectorResponseError(
                    "FRED normalized output "
                    "is missing "
                    "observation_position."
                )

            duplicate_positions = (
                dataframe.duplicated(
                    subset=[
                        "series_id",
                        "observation_position",
                    ],
                    keep=False,
                )
            )

            if duplicate_positions.any():

                duplicate_records = (
                    dataframe.loc[
                        duplicate_positions,
                        [
                            "series_id",
                            "observation_position",
                        ],
                    ]
                    .drop_duplicates()
                    .to_dict(
                        orient="records"
                    )
                )

                raise ConnectorResponseError(
                    "FRED normalized output "
                    "contains duplicate technical "
                    "observation positions: "
                    f"{duplicate_records}"
                )

        # --------------------------------------------------------------------
        # METADATA
        # --------------------------------------------------------------------

        result_metadata = {
            "series_count":
                len(
                    series_ids
                ),
            "http_requests":
                total_requests,
            "observation_records":
                total_observations,
            "series":
                series_metadata,
        }

        logger.info(
            "FRED acquisition completed: "
            "Series=%s; "
            "Requests=%s; "
            "Observations=%s",
            result_metadata[
                "series_count"
            ],
            result_metadata[
                "http_requests"
            ],
            result_metadata[
                "observation_records"
            ],
        )

        return ConnectorResult(
            data=dataframe,
            metadata=result_metadata,
        )