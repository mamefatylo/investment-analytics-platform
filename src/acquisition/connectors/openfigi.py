"""
openfigi.py

Source-faithful OpenFIGI mapping connector.

Purpose
-------
Acquire OpenFIGI mapping data without applying downstream
business-selection rules.

Architecture
------------
Upstream Dataset
        |
        v
Generic API Engine
        |
        v
ConnectorContext
        |
        v
OpenFigiConnector
        |
        v
OpenFIGI Mapping API
        |
        v
Source-faithful tabular mapping data
        |
        v
ConnectorResult
        |
        v
Generic API Engine
        |
        v
Raw Dataset

Responsibilities
----------------
The connector is responsible for:
- validating OpenFIGI-specific Parameters;
- validating the upstream input contract declared by Parameters;
- loading OpenFIGI authentication material from the environment;
- constructing OpenFIGI mapping jobs;
- performing provider-specific batching;
- executing mapping requests;
- handling OpenFIGI rate-limit responses;
- interpreting OpenFIGI response structures;
- preserving all mapping results returned by OpenFIGI;
- preserving OpenFIGI warning and error responses;
- returning technically normalized tabular data;
- returning non-secret execution metadata.

The connector does not:
- read the Source Registry;
- contain business Source IDs;
- contain business Dataset IDs;
- contain the OpenFIGI endpoint;
- contain output file names;
- contain storage directories;
- resolve output paths;
- publish files;
- log acquisitions;
- load SQLite tables;
- determine the best FIGI;
- filter market sectors;
- filter security types;
- classify mappings as matched or unmatched;
- construct the Securities Master;
- perform portfolio or analytical filtering.

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
# OPENFIGI AUTHENTICATION CONTRACT
# ============================================================================

API_KEY_ENVIRONMENT_VARIABLE = "OPENFIGI_API_KEY"
API_KEY_HEADER = "X-OPENFIGI-APIKEY"


# ============================================================================
# PROVIDER LIMITS
# ============================================================================

AUTHENTICATED_MAX_JOBS_PER_REQUEST = 100

RETRYABLE_STATUS_CODES = frozenset(
    {
        429,
        500,
        502,
        503,
        504,
    }
)


# ============================================================================
# TECHNICAL RUNTIME DEFAULTS
# ============================================================================

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_READ_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 2.0

DEFAULT_USER_AGENT = (
    "investment-data-pipeline/1.0"
)


# ============================================================================
# OPENFIGI PARAMETER CONTRACT
# ============================================================================

ID_TYPE_PARAMETER = "id_type"
ID_VALUE_COLUMN_PARAMETER = "id_value_column"

REQUIRED_PARAMETERS = (
    ID_TYPE_PARAMETER,
    ID_VALUE_COLUMN_PARAMETER,
)


# ============================================================================
# NORMALIZATION
# ============================================================================

def optional_text(
    value: Any,
) -> str | None:
    """
    Normalize optional scalar text.
    """

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

    normalized = str(
        value
    ).strip()

    return normalized or None


def positive_integer(
    value: Any,
    field_name: str,
) -> int:
    """
    Normalize a strictly positive integer.
    """

    if isinstance(
        value,
        bool,
    ):

        raise ConnectorConfigurationError(
            f"{field_name} must be "
            "a positive integer."
        )

    try:

        normalized = int(
            value
        )

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


def positive_number(
    value: Any,
    field_name: str,
) -> float:
    """
    Normalize a strictly positive numeric value.
    """

    if isinstance(
        value,
        bool,
    ):

        raise ConnectorConfigurationError(
            f"{field_name} must be positive."
        )

    try:

        normalized = float(
            value
        )

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
    Load the OpenFIGI API key from the environment.

    The secret is never obtained from the Source Registry.
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
# RUNTIME OPTIONS
# ============================================================================

def resolve_runtime_options(
    runtime_options: dict[str, Any],
) -> dict[str, Any]:
    """
    Resolve technical execution settings.

    Runtime options are supplied by the acquisition engine,
    not by Dataset Parameters.
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
        set(
            runtime_options
        )
        - allowed_options
    )

    if unsupported:

        raise ConnectorConfigurationError(
            "Unsupported OpenFIGI runtime options: "
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
# INPUT CONTRACT
# ============================================================================

def validate_input_data(
    input_data: pd.DataFrame,
    id_value_column: str,
) -> None:
    """
    Validate the upstream input required to build mapping jobs.

    The identifier column name is configuration-driven and therefore
    is not embedded in this connector.
    """

    if not isinstance(
        input_data,
        pd.DataFrame,
    ):

        raise ConnectorConfigurationError(
            "OpenFIGI requires input_data "
            "as a pandas DataFrame."
        )

    if input_data.empty:

        raise ConnectorConfigurationError(
            "OpenFIGI input_data is empty."
        )

    if (
        id_value_column
        not in input_data.columns
    ):

        raise ConnectorConfigurationError(
            "Configured OpenFIGI identifier column "
            "was not found in input_data: "
            f"{id_value_column}"
        )

    normalized_identifiers = (
        input_data[
            id_value_column
        ]
        .apply(
            optional_text
        )
    )

    missing_mask = (
        normalized_identifiers
        .isna()
    )

    if missing_mask.any():

        rows = [
            int(index) + 2
            for index
            in normalized_identifiers[
                missing_mask
            ].index
        ]

        raise ConnectorConfigurationError(
            "OpenFIGI input contains missing "
            "identifier values at input rows: "
            f"{rows}"
        )


# ============================================================================
# REQUEST JOBS
# ============================================================================

def build_mapping_jobs(
    input_data: pd.DataFrame,
    *,
    id_type: str,
    id_value_column: str,
) -> list[dict[str, Any]]:
    """
    Build ordered OpenFIGI mapping jobs.

    Each input row produces exactly one mapping job.

    No provider filter such as exchange, security type, currency,
    or market sector is added implicitly.
    """

    jobs = []

    for row_position, (
        source_index,
        row,
    ) in enumerate(
        input_data.iterrows()
    ):

        identifier = optional_text(
            row[
                id_value_column
            ]
        )

        if identifier is None:

            raise ConnectorConfigurationError(
                "OpenFIGI identifier cannot "
                f"be empty at row {source_index}."
            )

        payload = {
            "idType":
                id_type,
            "idValue":
                identifier,
        }

        jobs.append(
            {
                "input_position":
                    row_position,
                "input_index":
                    source_index,
                "identifier_type":
                    id_type,
                "identifier_value":
                    identifier,
                "payload":
                    payload,
            }
        )

    return jobs


# ============================================================================
# BATCHING
# ============================================================================

def chunked(
    items: list,
    size: int,
):
    """
    Yield ordered item batches.
    """

    if size <= 0:

        raise ValueError(
            "Batch size must be positive."
        )

    for start in range(
        0,
        len(items),
        size,
    ):

        yield items[
            start:
            start + size
        ]


# ============================================================================
# RETRY DELAY
# ============================================================================

def resolve_retry_delay(
    response: requests.Response,
    *,
    attempt: int,
    backoff_seconds: float,
) -> float:
    """
    Resolve provider-directed or exponential retry delay.

    OpenFIGI rate-limit headers are preferred when available.
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

    rate_limit_reset = optional_text(
        response.headers.get(
            "ratelimit-reset"
        )
    )

    if rate_limit_reset is not None:

        try:

            return max(
                float(
                    rate_limit_reset
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


# ============================================================================
# HTTP REQUEST
# ============================================================================

def request_mapping_batch(
    *,
    session: requests.Session,
    endpoint: str,
    api_key: str,
    jobs: list[dict[str, Any]],
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    backoff_seconds: float,
) -> list:
    """
    Submit one OpenFIGI mapping batch.

    The returned list must preserve one response item per
    submitted mapping job.
    """

    payload = [
        job[
            "payload"
        ]
        for job in jobs
    ]

    headers = {
        "Accept":
            "application/json",
        "Content-Type":
            "application/json",
        API_KEY_HEADER:
            api_key,
    }

    timeout = (
        connect_timeout,
        read_timeout,
    )

    for attempt in range(
        1,
        max_attempts + 1,
    ):

        try:

            response = session.post(
                endpoint,
                headers=headers,
                json=payload,
                timeout=timeout,
            )

        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:

            if attempt == max_attempts:

                raise ConnectorTransportError(
                    "OpenFIGI transport request "
                    "failed after all retries."
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
                "OpenFIGI transport retry "
                "%s/%s in %.2f seconds",
                attempt,
                max_attempts,
                wait_seconds,
            )

            time.sleep(
                wait_seconds
            )

            continue

        if (
            response.status_code
            in RETRYABLE_STATUS_CODES
        ):

            if attempt == max_attempts:

                raise ConnectorTransportError(
                    "OpenFIGI returned retryable "
                    "HTTP status after all attempts: "
                    f"{response.status_code}"
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
                "OpenFIGI HTTP retry "
                "%s/%s after status %s "
                "in %.2f seconds",
                attempt,
                max_attempts,
                response.status_code,
                wait_seconds,
            )

            time.sleep(
                wait_seconds
            )

            continue

        try:

            response.raise_for_status()

        except requests.HTTPError as error:

            if (
                response.status_code
                in {
                    401,
                    403,
                }
            ):

                raise ConnectorAuthenticationError(
                    "OpenFIGI authentication "
                    "was rejected."
                ) from error

            raise ConnectorTransportError(
                "OpenFIGI request failed with "
                f"HTTP {response.status_code}."
            ) from error

        try:

            data = response.json()

        except requests.JSONDecodeError as error:

            raise ConnectorResponseError(
                "OpenFIGI response is not "
                "valid JSON."
            ) from error

        if not isinstance(
            data,
            list,
        ):

            raise ConnectorResponseError(
                "OpenFIGI mapping response "
                "must be a list."
            )

        if len(data) != len(jobs):

            raise ConnectorResponseError(
                "OpenFIGI response count does "
                "not match submitted job count. "
                f"Jobs={len(jobs)}; "
                f"Responses={len(data)}"
            )

        return data

    raise ConnectorTransportError(
        "OpenFIGI request exhausted "
        "all configured attempts."
    )


# ============================================================================
# RAW RESPONSE NORMALIZATION
# ============================================================================

def build_base_output_record(
    job: dict[str, Any],
) -> dict[str, Any]:
    """
    Build technical lineage fields for one OpenFIGI mapping response.
    """

    return {
        "input_position":
            job[
                "input_position"
            ],
        "input_index":
            job[
                "input_index"
            ],
        "id_type":
            job[
                "identifier_type"
            ],
        "id_value":
            job[
                "identifier_value"
            ],
    }


def normalize_response_item(
    job: dict[str, Any],
    response_item: Any,
) -> list[dict[str, Any]]:
    """
    Normalize one OpenFIGI job response.

    All mapping records returned in response_item["data"] are preserved.

    Warning and error responses are also preserved as rows so that
    acquisition does not silently discard provider information.
    """

    base_record = build_base_output_record(
        job
    )

    if not isinstance(
        response_item,
        dict,
    ):

        raise ConnectorResponseError(
            "OpenFIGI response item must "
            "be a JSON object."
        )

    provider_error = optional_text(
        response_item.get(
            "error"
        )
    )

    provider_warning = optional_text(
        response_item.get(
            "warning"
        )
    )

    provider_data = response_item.get(
        "data"
    )

    output_records = []

    if provider_data is not None:

        if not isinstance(
            provider_data,
            list,
        ):

            raise ConnectorResponseError(
                "OpenFIGI data property must "
                "be a list when provided."
            )

        for result_position, mapping in enumerate(
            provider_data
        ):

            if not isinstance(
                mapping,
                dict,
            ):

                raise ConnectorResponseError(
                    "OpenFIGI mapping result "
                    "must be a JSON object."
                )

            record = (
                base_record.copy()
            )

            record.update(
                {
                    "result_position":
                        result_position,
                    "figi":
                        mapping.get(
                            "figi"
                        ),
                    "ticker":
                        mapping.get(
                            "ticker"
                        ),
                    "name":
                        mapping.get(
                            "name"
                        ),
                    "exch_code":
                        mapping.get(
                            "exchCode"
                        ),
                    "market_sector":
                        mapping.get(
                            "marketSector"
                        ),
                    "security_type":
                        mapping.get(
                            "securityType"
                        ),
                    "security_type_2":
                        mapping.get(
                            "securityType2"
                        ),
                    "security_description":
                        mapping.get(
                            "securityDescription"
                        ),
                    "composite_figi":
                        mapping.get(
                            "compositeFIGI"
                        ),
                    "share_class_figi":
                        mapping.get(
                            "shareClassFIGI"
                        ),
                    "provider_warning":
                        provider_warning,
                    "provider_error":
                        provider_error,
                }
            )

            output_records.append(
                record
            )

    if output_records:

        return output_records

    record = (
        base_record.copy()
    )

    record.update(
        {
            "result_position":
                None,
            "figi":
                None,
            "ticker":
                None,
            "name":
                None,
            "exch_code":
                None,
            "market_sector":
                None,
            "security_type":
                None,
            "security_type_2":
                None,
            "security_description":
                None,
            "composite_figi":
                None,
            "share_class_figi":
                None,
            "provider_warning":
                provider_warning,
            "provider_error":
                provider_error,
        }
    )

    output_records.append(
        record
    )

    return output_records


# ============================================================================
# OUTPUT CONTRACT
# ============================================================================

OUTPUT_COLUMNS = (
    "input_position",
    "input_index",
    "id_type",
    "id_value",
    "result_position",
    "figi",
    "ticker",
    "name",
    "exch_code",
    "market_sector",
    "security_type",
    "security_type_2",
    "security_description",
    "composite_figi",
    "share_class_figi",
    "provider_warning",
    "provider_error",
)


def build_output_frame(
    records: list[dict[str, Any]],
) -> pd.DataFrame:
    """
    Build the final source-faithful normalized DataFrame.
    """

    dataframe = pd.DataFrame(
        records,
        columns=OUTPUT_COLUMNS,
    )

    if dataframe.empty:

        raise ConnectorResponseError(
            "OpenFIGI produced no "
            "normalized response records."
        )

    return (
        dataframe
        .reset_index(
            drop=True
        )
    )


# ============================================================================
# CONNECTOR
# ============================================================================

class OpenFigiConnector(
    BaseConnector
):
    
    """
    Source-faithful OpenFIGI mapping connector.

    The connector acquires every mapping returned by OpenFIGI
    for each configured identifier.

    It performs no downstream business selection.
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
        Validate OpenFIGI-specific Dataset Parameters.
        """

        if not isinstance(
            parameters,
            dict,
        ):

            raise ConnectorConfigurationError(
                "OpenFIGI Parameters must "
                "be a dictionary."
            )

        missing_parameters = [
            parameter
            for parameter
            in REQUIRED_PARAMETERS
            if optional_text(
                parameters.get(
                    parameter
                )
            )
            is None
        ]

        if missing_parameters:

            raise ConnectorConfigurationError(
                "OpenFIGI Parameters are missing: "
                f"{missing_parameters}"
            )

        allowed_parameters = {
            ID_TYPE_PARAMETER,
            ID_VALUE_COLUMN_PARAMETER,
        }

        unsupported_parameters = sorted(
            set(
                parameters
            )
            - allowed_parameters
        )

        if unsupported_parameters:

            raise ConnectorConfigurationError(
                "Unsupported OpenFIGI "
                "Dataset Parameters: "
                f"{unsupported_parameters}"
            )


    @classmethod
    def validate_context(
        cls,
        context: ConnectorContext,
    ) -> None:
        """
        Validate the complete OpenFIGI execution context.
        """

        super().validate_context(
            context
        )

        endpoint = optional_text(
            context.endpoint
        )

        if endpoint is None:

            raise ConnectorConfigurationError(
                "OpenFIGI requires "
                "Access Endpoint."
            )

        if context.input_data is None:

            raise ConnectorConfigurationError(
                "OpenFIGI requires "
                "an upstream input Dataset."
            )

        id_value_column = optional_text(
            context.parameters.get(
                ID_VALUE_COLUMN_PARAMETER
            )
        )

        if id_value_column is None:

            raise ConnectorConfigurationError(
                "OpenFIGI requires "
                "id_value_column."
            )

        validate_input_data(
            context.input_data,
            id_value_column,
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
        Acquire all OpenFIGI mappings for the configured input.

        Returns source-faithful normalized mapping records.
        """

        cls.validate_context(
            context
        )

        endpoint = optional_text(
            context.endpoint
        )

        if endpoint is None:

            raise ConnectorConfigurationError(
                "OpenFIGI requires "
                "Access Endpoint."
            )

        parameters = (
            context.parameters.copy()
        )

        runtime = resolve_runtime_options(
            context.runtime_options
        )

        id_type = optional_text(
            parameters.get(
                ID_TYPE_PARAMETER
            )
        )

        id_value_column = optional_text(
            parameters.get(
                ID_VALUE_COLUMN_PARAMETER
            )
        )

        if (
            id_type is None
            or id_value_column is None
        ):

            raise ConnectorConfigurationError(
                "OpenFIGI identifier "
                "configuration is incomplete."
            )

        input_data = (
            context.input_data.copy(
                deep=True
            )
        )

        jobs = build_mapping_jobs(
            input_data,
            id_type=id_type,
            id_value_column=(
                id_value_column
            ),
        )

        if not jobs:

            raise ConnectorConfigurationError(
                "OpenFIGI produced no "
                "mapping jobs."
            )

        api_key = load_api_key()

        records = []

        request_count = 0

        provider_warning_count = 0
        provider_error_count = 0

        with requests.Session() as session:

            session.headers.update(
                {
                    "User-Agent":
                        runtime[
                            "user_agent"
                        ],
                }
            )

            for batch_number, batch in enumerate(
                chunked(
                    jobs,
                    AUTHENTICATED_MAX_JOBS_PER_REQUEST,
                ),
                start=1,
            ):

                logger.info(
                    "Submitting OpenFIGI batch "
                    "%s with %s mapping jobs",
                    batch_number,
                    len(batch),
                )

                responses = (
                    request_mapping_batch(
                        session=session,
                        endpoint=endpoint,
                        api_key=api_key,
                        jobs=batch,
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
                )

                request_count += 1

                for job, response_item in zip(
                    batch,
                    responses,
                    strict=True,
                ):

                    if isinstance(
                        response_item,
                        dict,
                    ):

                        if optional_text(
                            response_item.get(
                                "warning"
                            )
                        ) is not None:

                            provider_warning_count += 1

                        if optional_text(
                            response_item.get(
                                "error"
                            )
                        ) is not None:

                            provider_error_count += 1

                    records.extend(
                        normalize_response_item(
                            job,
                            response_item,
                        )
                    )

        dataframe = build_output_frame(
            records
        )

        mapping_count = int(
            dataframe[
                "figi"
            ]
            .notna()
            .sum()
        )

        metadata = {
            "input_records":
                len(
                    input_data
                ),
            "mapping_jobs":
                len(
                    jobs
                ),
            "http_requests":
                request_count,
            "normalized_records":
                len(
                    dataframe
                ),
            "mapping_records":
                mapping_count,
            "provider_warnings":
                provider_warning_count,
            "provider_errors":
                provider_error_count,
        }

        logger.info(
            "OpenFIGI acquisition completed: "
            "Input=%s; Jobs=%s; "
            "Requests=%s; Records=%s; "
            "Mappings=%s; Warnings=%s; "
            "Errors=%s",
            metadata[
                "input_records"
            ],
            metadata[
                "mapping_jobs"
            ],
            metadata[
                "http_requests"
            ],
            metadata[
                "normalized_records"
            ],
            metadata[
                "mapping_records"
            ],
            metadata[
                "provider_warnings"
            ],
            metadata[
                "provider_errors"
            ],
        )

        return ConnectorResult(
            data=dataframe,
            metadata=metadata,
        )