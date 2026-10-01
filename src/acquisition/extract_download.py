"""
extract_download.py

Generic registry-driven HTTP Download acquisition engine.

Purpose
-------
Execute Download Datasets declared in the validated Source Registry.

Architecture
------------
Source Registry
        |
        v
validate_source_registry.py
        |
        v
source_registry.py
        |
        v
extract_download.py
        |
        +--> select active Download Dataset
        |
        +--> resolve Access Endpoint
        |
        +--> resolve output path
        |
        +--> stream HTTP response
        |
        +--> write staging file
        |
        +--> validate downloaded artifact
        |
        +--> atomic publication
        |
        +--> acquisition logging

Design principles
-----------------
- no provider-specific logic;
- no business Source ID hardcoding;
- no business Dataset ID hardcoding;
- no endpoint hardcoding;
- no output-path hardcoding;
- no secrets;
- no business transformation;
- streaming downloads;
- bounded retries;
- bounded response size;
- same-directory staging;
- atomic publication;
- existing published Dataset remains intact when acquisition fails;
- failures are isolated per Dataset unless fail-fast is requested.

Download is intentionally byte-preserving. The downloaded artifact
is not converted, parsed, reformatted, or otherwise transformed by
this acquisition engine.
"""

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd
import requests

from src.acquisition.acquisition_logger import log_acquisition
from src.governance.source_registry import (
    DATASET_KEY,
    build_dataset_absolute_path,
    build_resolved_registry,
    get_access_endpoint,
    get_download_enabled_datasets,
    get_parameters,
)
from src.utils.logger import logger


# ============================================================================
# EXECUTION STATUS
# ============================================================================

SUCCESS_STATUS = "Success"
FAILED_STATUS = "Failed"


# ============================================================================
# HTTP CONTRACT
# ============================================================================

RETRYABLE_STATUS_CODES = frozenset(
    {
        408,
        425,
        429,
        500,
        502,
        503,
        504,
    }
)

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_READ_TIMEOUT_SECONDS = 120.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_CHUNK_SIZE_BYTES = 1024 * 1024
DEFAULT_MAX_DOWNLOAD_SIZE_BYTES = 500 * 1024 * 1024

DEFAULT_USER_AGENT = (
    "investment-data-pipeline/1.0"
)


# ============================================================================
# DATASET PARAMETER CONTRACT
# ============================================================================

HEADERS_PARAMETER = "headers"
MAX_DOWNLOAD_SIZE_PARAMETER = "max_download_size_bytes"

SUPPORTED_DOWNLOAD_PARAMETERS = frozenset(
    {
        HEADERS_PARAMETER,
        MAX_DOWNLOAD_SIZE_PARAMETER,
    }
)

SENSITIVE_HEADER_NAMES = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "x-api-key",
        "x-auth-token",
        "cookie",
        "set-cookie",
    }
)


# ============================================================================
# EXECUTION MODEL
# ============================================================================

@dataclass(frozen=True)
class DatasetExecution:
    """
    Fully resolved Download Dataset execution contract.
    """

    configuration: dict[str, Any]
    endpoint: str
    parameters: dict[str, Any]
    output_path: Path


@dataclass(frozen=True)
class DownloadResult:
    """
    Technical result of one successful HTTP download.
    """

    staging_path: Path
    bytes_written: int
    status_code: int
    content_type: str | None
    content_length: int | None
    final_url: str


# ============================================================================
# COMMAND LINE
# ============================================================================

def parse_arguments():
    """
    Parse Download engine command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Execute active Download Datasets "
            "configured in the Source Registry."
        )
    )

    parser.add_argument(
        "--dataset-id",
        default=None,
        help=(
            "Execute one active Download Dataset only. "
            "When omitted, all active Download Datasets "
            "are executed."
        ),
    )

    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=(
            DEFAULT_CONNECT_TIMEOUT_SECONDS
        ),
    )

    parser.add_argument(
        "--read-timeout",
        type=float,
        default=(
            DEFAULT_READ_TIMEOUT_SECONDS
        ),
    )

    parser.add_argument(
        "--max-attempts",
        type=int,
        default=(
            DEFAULT_MAX_ATTEMPTS
        ),
    )

    parser.add_argument(
        "--backoff-seconds",
        type=float,
        default=(
            DEFAULT_BACKOFF_SECONDS
        ),
    )

    parser.add_argument(
        "--chunk-size-bytes",
        type=int,
        default=(
            DEFAULT_CHUNK_SIZE_BYTES
        ),
    )

    parser.add_argument(
        "--max-download-size-bytes",
        type=int,
        default=(
            DEFAULT_MAX_DOWNLOAD_SIZE_BYTES
        ),
    )

    parser.add_argument(
        "--user-agent",
        default=(
            DEFAULT_USER_AGENT
        ),
    )

    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help=(
            "Stop immediately when a Dataset fails. "
            "Default behavior continues with other "
            "independent Download Datasets."
        ),
    )

    return parser.parse_args()


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

        raise ValueError(
            f"{field_name} must be "
            "a positive number."
        )

    try:

        normalized = float(
            value
        )

    except (
        TypeError,
        ValueError,
    ) as error:

        raise ValueError(
            f"{field_name} must be numeric."
        ) from error

    if normalized <= 0:

        raise ValueError(
            f"{field_name} must be "
            "greater than zero."
        )

    return normalized


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

        raise ValueError(
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

        raise ValueError(
            f"{field_name} must be "
            "a positive integer."
        ) from error

    if normalized <= 0:

        raise ValueError(
            f"{field_name} must be "
            "greater than zero."
        )

    return normalized


def normalize_url(
    value: Any,
) -> str:
    """
    Normalize a required HTTP endpoint.
    """

    endpoint = optional_text(
        value
    )

    if endpoint is None:

        raise ValueError(
            "Access Endpoint cannot be empty."
        )

    lowered = endpoint.casefold()

    if not (
        lowered.startswith(
            "https://"
        )
        or lowered.startswith(
            "http://"
        )
    ):

        raise ValueError(
            "Download Access Endpoint must "
            "use HTTP or HTTPS."
        )

    return endpoint


# ============================================================================
# DOWNLOAD PARAMETERS
# ============================================================================

def validate_download_parameters(
    parameters: dict[str, Any],
) -> None:
    """
    Validate generic Download Dataset Parameters.

    Download Parameters intentionally remain minimal.

    headers
        Optional non-secret HTTP request headers.

    max_download_size_bytes
        Optional Dataset-specific safety limit.

    Authentication secrets should not be stored in the registry.
    """

    if not isinstance(
        parameters,
        dict,
    ):

        raise TypeError(
            "Download Parameters must "
            "be a dictionary."
        )

    unsupported = sorted(
        set(
            parameters
        )
        - SUPPORTED_DOWNLOAD_PARAMETERS
    )

    if unsupported:

        raise ValueError(
            "Unsupported Download Parameters: "
            f"{unsupported}"
        )

    headers = parameters.get(
        HEADERS_PARAMETER
    )

    if headers is not None:

        if not isinstance(
            headers,
            dict,
        ):

            raise ValueError(
                "Download headers must be "
                "a JSON object."
            )

        for key, value in headers.items():

            header_name = optional_text(
                key
            )

            header_value = optional_text(
                value
            )

            if header_name is None:

                raise ValueError(
                    "Download header name "
                    "cannot be empty."
                )

            if header_value is None:

                raise ValueError(
                    "Download header value "
                    f"cannot be empty: {header_name}"
                )

            if (
                header_name.casefold()
                in SENSITIVE_HEADER_NAMES
            ):

                raise ValueError(
                    "Sensitive authentication "
                    "headers cannot be stored in "
                    "Download Parameters: "
                    f"{header_name}"
                )

    if (
        MAX_DOWNLOAD_SIZE_PARAMETER
        in parameters
    ):

        positive_integer(
            parameters[
                MAX_DOWNLOAD_SIZE_PARAMETER
            ],
            MAX_DOWNLOAD_SIZE_PARAMETER,
        )


def build_request_headers(
    parameters: dict[str, Any],
    *,
    user_agent: str,
) -> dict[str, str]:
    """
    Build safe generic HTTP request headers.
    """

    normalized_user_agent = optional_text(
        user_agent
    )

    if normalized_user_agent is None:

        raise ValueError(
            "user_agent cannot be empty."
        )

    headers = {
        "User-Agent":
            normalized_user_agent,
        "Accept":
            "*/*",
    }

    configured_headers = parameters.get(
        HEADERS_PARAMETER,
        {},
    )

    if configured_headers is None:

        configured_headers = {}

    for key, value in (
        configured_headers.items()
    ):

        header_name = optional_text(
            key
        )

        header_value = optional_text(
            value
        )

        if (
            header_name is None
            or header_value is None
        ):

            raise ValueError(
                "Configured HTTP headers "
                "must contain non-empty "
                "names and values."
            )

        headers[
            header_name
        ] = header_value

    return headers


def resolve_max_download_size(
    parameters: dict[str, Any],
    *,
    default_limit: int,
) -> int:
    """
    Resolve the effective download-size safety limit.
    """

    configured = parameters.get(
        MAX_DOWNLOAD_SIZE_PARAMETER
    )

    if configured is None:

        return positive_integer(
            default_limit,
            "max_download_size_bytes",
        )

    return positive_integer(
        configured,
        MAX_DOWNLOAD_SIZE_PARAMETER,
    )


# ============================================================================
# DATASET SELECTION
# ============================================================================

def select_download_datasets(
    registry: pd.DataFrame,
    dataset_id: str | None = None,
) -> pd.DataFrame:
    """
    Select active Download Datasets for execution.
    """

    if not isinstance(
        registry,
        pd.DataFrame,
    ):

        raise TypeError(
            "registry must be "
            "a pandas DataFrame."
        )

    datasets = (
        get_download_enabled_datasets(
            registry=registry
        )
    )

    requested_dataset_id = optional_text(
        dataset_id
    )

    if requested_dataset_id is None:

        return (
            datasets
            .copy(
                deep=True
            )
            .reset_index(
                drop=True
            )
        )

    matches = datasets[
        datasets[
            DATASET_KEY
        ]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.casefold()
        .eq(
            requested_dataset_id.casefold()
        )
    ]

    if matches.empty:

        raise KeyError(
            "Requested Dataset is not "
            "an active Download Dataset: "
            f"{requested_dataset_id}"
        )

    if len(
        matches
    ) > 1:

        raise RuntimeError(
            "Requested Dataset resolved to "
            "multiple Download configurations: "
            f"{requested_dataset_id}"
        )

    return (
        matches
        .copy(
            deep=True
        )
        .reset_index(
            drop=True
        )
    )


# ============================================================================
# EXECUTION RESOLUTION
# ============================================================================

def resolve_dataset_execution(
    configuration: dict[str, Any],
) -> DatasetExecution:
    """
    Resolve one Download Dataset execution.
    """

    if not isinstance(
        configuration,
        dict,
    ):

        raise TypeError(
            "configuration must be "
            "a dictionary."
        )

    dataset_id = optional_text(
        configuration.get(
            DATASET_KEY
        )
    )

    if dataset_id is None:

        raise ValueError(
            "Dataset configuration is "
            "missing Dataset ID."
        )

    endpoint = normalize_url(
        get_access_endpoint(
            configuration
        )
    )

    parameters = get_parameters(
        configuration
    )

    validate_download_parameters(
        parameters
    )

    output_path = (
        build_dataset_absolute_path(
            configuration
        )
    )

    return DatasetExecution(
        configuration=(
            configuration.copy()
        ),
        endpoint=endpoint,
        parameters=parameters,
        output_path=output_path,
    )


# ============================================================================
# STAGING
# ============================================================================

def create_staging_path(
    output_path: Path,
) -> Path:
    """
    Create a temporary staging file beside the final output.

    Same-directory staging keeps the temporary artifact on the same
    filesystem as the target under normal operation.
    """

    if not isinstance(
        output_path,
        Path,
    ):

        raise TypeError(
            "output_path must be "
            "a pathlib.Path."
        )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with NamedTemporaryFile(
        mode="wb",
        prefix=(
            f".{output_path.stem}_"
        ),
        suffix=output_path.suffix,
        dir=output_path.parent,
        delete=False,
    ) as temporary_file:

        path = Path(
            temporary_file.name
        )

    return path


# ============================================================================
# HTTP RESPONSE HELPERS
# ============================================================================

def parse_content_length(
    response: requests.Response,
) -> int | None:
    """
    Parse an optional HTTP Content-Length header.
    """

    value = optional_text(
        response.headers.get(
            "Content-Length"
        )
    )

    if value is None:
        return None

    try:

        length = int(
            value
        )

    except (
        TypeError,
        ValueError,
    ):

        return None

    if length < 0:

        return None

    return length


def resolve_retry_delay(
    response: requests.Response,
    *,
    attempt: int,
    backoff_seconds: float,
) -> float:
    """
    Resolve Retry-After or exponential backoff.
    """

    retry_after = optional_text(
        response.headers.get(
            "Retry-After"
        )
    )

    if retry_after is not None:

        try:

            numeric_delay = float(
                retry_after
            )

            return max(
                numeric_delay,
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


def response_content_type(
    response: requests.Response,
) -> str | None:
    """
    Return the normalized HTTP Content-Type.
    """

    content_type = optional_text(
        response.headers.get(
            "Content-Type"
        )
    )

    if content_type is None:

        return None

    return (
        content_type
        .split(
            ";",
            1,
        )[0]
        .strip()
        .casefold()
    )


# ============================================================================
# CONTENT VALIDATION
# ============================================================================

def looks_like_html(
    prefix: bytes,
) -> bool:
    """
    Return True when initial bytes strongly resemble HTML.

    This is a conservative protection against web/error pages
    accidentally published as binary data files.
    """

    if not prefix:

        return False

    normalized = (
        prefix[
            :4096
        ]
        .lstrip()
        .lower()
    )

    html_markers = (
        b"<!doctype html",
        b"<html",
        b"<head",
        b"<body",
    )

    return any(
        normalized.startswith(
            marker
        )
        for marker
        in html_markers
    )


def validate_download_content(
    *,
    staging_path: Path,
    output_path: Path,
    content_type: str | None,
    bytes_written: int,
) -> None:
    """
    Validate the downloaded artifact before publication.

    The validation is intentionally technical rather than
    provider-specific.
    """

    if bytes_written <= 0:

        raise RuntimeError(
            "Downloaded Dataset contains "
            "zero bytes."
        )

    if not staging_path.exists():

        raise FileNotFoundError(
            "Download staging file "
            "does not exist."
        )

    actual_size = (
        staging_path
        .stat()
        .st_size
    )

    if actual_size != bytes_written:

        raise RuntimeError(
            "Downloaded byte count does not "
            "match staging file size. "
            f"Written={bytes_written}; "
            f"Actual={actual_size}"
        )

    with staging_path.open(
        "rb"
    ) as handle:

        prefix = handle.read(
            4096
        )

    extension = (
        output_path
        .suffix
        .casefold()
    )

    html_content_types = {
        "text/html",
        "application/xhtml+xml",
    }

    if (
        content_type
        in html_content_types
        and extension
        not in {
            ".html",
            ".htm",
        }
    ):

        raise RuntimeError(
            "Download returned HTML content "
            "for a non-HTML Dataset."
        )

    if (
        looks_like_html(
            prefix
        )
        and extension
        not in {
            ".html",
            ".htm",
        }
    ):

        raise RuntimeError(
            "Downloaded artifact appears to "
            "contain HTML instead of the "
            "configured Dataset format."
        )


# ============================================================================
# STREAMING DOWNLOAD
# ============================================================================

def stream_response_to_file(
    response: requests.Response,
    *,
    staging_path: Path,
    chunk_size_bytes: int,
    max_download_size_bytes: int,
) -> int:
    """
    Stream an HTTP response body into the staging file.

    The size limit is enforced while streaming, including when
    Content-Length is unavailable or inaccurate.

    Returns
    -------
    int
        Number of bytes written to the staging file.
    """

    if not isinstance(
        response,
        requests.Response,
    ):
        raise TypeError(
            "response must be a requests.Response."
        )

    if not isinstance(
        staging_path,
        Path,
    ):
        raise TypeError(
            "staging_path must be a pathlib.Path."
        )

    chunk_size_bytes = positive_integer(
        chunk_size_bytes,
        "chunk_size_bytes",
    )

    max_download_size_bytes = positive_integer(
        max_download_size_bytes,
        "max_download_size_bytes",
    )

    content_length = parse_content_length(
        response
    )

    if (
        content_length is not None
        and content_length
        > max_download_size_bytes
    ):
        raise RuntimeError(
            "HTTP Content-Length exceeds "
            "the configured download limit. "
            f"ContentLength={content_length}; "
            f"Limit={max_download_size_bytes}"
        )

    bytes_written = 0

    try:

        with staging_path.open(
            "wb"
        ) as handle:

            for chunk in response.iter_content(
                chunk_size=chunk_size_bytes
            ):

                if not chunk:
                    continue

                next_size = (
                    bytes_written
                    + len(chunk)
                )

                if (
                    next_size
                    > max_download_size_bytes
                ):
                    raise RuntimeError(
                        "Downloaded content exceeds "
                        "the configured size limit. "
                        f"ReceivedAtLeast={next_size}; "
                        f"Limit={max_download_size_bytes}"
                    )

                handle.write(
                    chunk
                )

                bytes_written = (
                    next_size
                )

            handle.flush()

            os.fsync(
                handle.fileno()
            )

    except Exception:

        staging_path.unlink(
            missing_ok=True
        )

        raise

    return bytes_written


# ============================================================================
# DOWNLOAD
# ============================================================================

def download_dataset(
    execution: DatasetExecution,
    *,
    session: requests.Session,
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    backoff_seconds: float,
    chunk_size_bytes: int,
    max_download_size_bytes: int,
    user_agent: str,
) -> DownloadResult:
    """
    Download one Dataset into a staging file.

    Retryable transport errors and HTTP statuses are retried.
    The published output is never modified by this function.
    """

    staging_path = None

    headers = build_request_headers(
        execution.parameters,
        user_agent=user_agent,
    )

    effective_size_limit = (
        resolve_max_download_size(
            execution.parameters,
            default_limit=(
                max_download_size_bytes
            ),
        )
    )

    timeout = (
        connect_timeout,
        read_timeout,
    )

    for attempt in range(
        1,
        max_attempts + 1,
    ):

        if staging_path is not None:

            staging_path.unlink(
                missing_ok=True
            )

            staging_path = None

        try:

            response = session.get(
                execution.endpoint,
                headers=headers,
                stream=True,
                timeout=timeout,
                allow_redirects=True,
            )

        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:

            if attempt == max_attempts:

                raise RuntimeError(
                    "Download transport failed "
                    "after all configured attempts."
                ) from error

            delay = (
                backoff_seconds
                * (
                    2 ** (
                        attempt - 1
                    )
                )
            )

            logger.warning(
                "Download transport retry "
                "%s/%s in %.2f seconds",
                attempt,
                max_attempts,
                delay,
            )

            time.sleep(
                delay
            )

            continue

        try:

            if (
                response.status_code
                in RETRYABLE_STATUS_CODES
            ):

                if attempt == max_attempts:

                    raise RuntimeError(
                        "Download returned retryable "
                        "HTTP status after all "
                        "configured attempts: "
                        f"{response.status_code}"
                    )

                delay = resolve_retry_delay(
                    response,
                    attempt=attempt,
                    backoff_seconds=(
                        backoff_seconds
                    ),
                )

                logger.warning(
                    "Download HTTP retry "
                    "%s/%s after status %s "
                    "in %.2f seconds",
                    attempt,
                    max_attempts,
                    response.status_code,
                    delay,
                )

                response.close()

                time.sleep(
                    delay
                )

                continue

            try:

                response.raise_for_status()

            except requests.HTTPError as error:

                raise RuntimeError(
                    "Download request failed "
                    "with HTTP status "
                    f"{response.status_code}."
                ) from error

            staging_path = (
                create_staging_path(
                    execution.output_path
                )
            )

            bytes_written = (
                stream_response_to_file(
                    response,
                    staging_path=(
                        staging_path
                    ),
                    chunk_size_bytes=(
                        chunk_size_bytes
                    ),
                    max_download_size_bytes=(
                        effective_size_limit
                    ),
                )
            )

            content_type = (
                response_content_type(
                    response
                )
            )

            content_length = (
                parse_content_length(
                    response
                )
            )

            final_url = str(
                response.url
            )

            validate_download_content(
                staging_path=staging_path,
                output_path=(
                    execution.output_path
                ),
                content_type=content_type,
                bytes_written=(
                    bytes_written
                ),
            )

            return DownloadResult(
                staging_path=staging_path,
                bytes_written=(
                    bytes_written
                ),
                status_code=(
                    int(
                        response.status_code
                    )
                ),
                content_type=(
                    content_type
                ),
                content_length=(
                    content_length
                ),
                final_url=(
                    final_url
                ),
            )

        except Exception:

            if staging_path is not None:

                staging_path.unlink(
                    missing_ok=True
                )

            raise

        finally:

            response.close()

    raise RuntimeError(
        "Download exhausted all "
        "configured attempts."
    )


# ============================================================================
# ATOMIC PUBLICATION
# ============================================================================

def publish_staged_file(
    staging_path: Path,
    output_path: Path,
) -> None:
    """
    Atomically replace the published Dataset.
    """

    if not staging_path.exists():

        raise FileNotFoundError(
            "Staging file does not exist: "
            f"{staging_path}"
        )

    if not staging_path.is_file():

        raise ValueError(
            "Staging path is not a file: "
            f"{staging_path}"
        )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    os.replace(
        staging_path,
        output_path,
    )


# ============================================================================
# AUDIT SAFETY
# ============================================================================

def sanitize_url_for_logging(
    url: str,
) -> str:
    """
    Remove query parameters from a URL before audit logging.

    Download endpoints may contain tokens or opaque request parameters.
    The acquisition log records only the URL before the query string.
    """

    normalized_url = optional_text(
        url
    )

    if normalized_url is None:

        return ""

    return (
        normalized_url
        .split(
            "?",
            1,
        )[0]
    )


def build_success_notes(
    execution: DatasetExecution,
    result: DownloadResult,
) -> str:
    """
    Build structured non-secret success notes.
    """

    payload = {
        "dataset_id":
            execution.configuration[
                DATASET_KEY
            ],
        "bytes_written":
            result.bytes_written,
        "http_status":
            result.status_code,
        "content_type":
            result.content_type,
        "content_length":
            result.content_length,
        "final_url":
            sanitize_url_for_logging(
                result.final_url
            ),
    }

    return json.dumps(
        payload,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
    )


def build_failure_notes(
    execution: DatasetExecution,
    error: Exception,
) -> str:
    """
    Build structured non-secret failure notes.
    """

    payload = {
        "dataset_id":
            execution.configuration[
                DATASET_KEY
            ],
        "error_type":
            type(
                error
            ).__name__,
        "error":
            str(
                error
            ),
    }

    return json.dumps(
        payload,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
    )


# ============================================================================
# ACQUISITION LOGGING
# ============================================================================

def log_dataset_event(
    *,
    output_path: Path,
    status: str,
    records_produced: int | None,
    notes: str,
):
    """
    Persist one Download Dataset acquisition event.
    """

    return log_acquisition(
        output_file=output_path,
        storage_location=(
            output_path.parent
        ),
        status=status,
        records_produced=(
            records_produced
        ),
        notes=notes,
    )


# ============================================================================
# SINGLE DATASET EXECUTION
# ============================================================================

def execute_download_dataset(
    execution: DatasetExecution,
    *,
    session: requests.Session,
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    backoff_seconds: float,
    chunk_size_bytes: int,
    max_download_size_bytes: int,
    user_agent: str,
) -> Path:
    """
    Execute one resolved Download Dataset.

    records_produced is intentionally None because a raw file download
    has no generic record concept before parsing.
    """

    dataset_id = (
        execution.configuration[
            DATASET_KEY
        ]
    )

    staging_path = None

    logger.info(
        "Download Dataset execution started: %s",
        dataset_id,
    )

    logger.info(
        "Download output resolved: %s",
        execution.output_path,
    )

    try:

        result = download_dataset(
            execution,
            session=session,
            connect_timeout=(
                connect_timeout
            ),
            read_timeout=(
                read_timeout
            ),
            max_attempts=(
                max_attempts
            ),
            backoff_seconds=(
                backoff_seconds
            ),
            chunk_size_bytes=(
                chunk_size_bytes
            ),
            max_download_size_bytes=(
                max_download_size_bytes
            ),
            user_agent=(
                user_agent
            ),
        )

        staging_path = (
            result.staging_path
        )

        publish_staged_file(
            staging_path,
            execution.output_path,
        )

        staging_path = None

        notes = build_success_notes(
            execution,
            result,
        )

        run_id = log_dataset_event(
            output_path=(
                execution.output_path
            ),
            status=SUCCESS_STATUS,
            records_produced=None,
            notes=notes,
        )

        logger.info(
            "Download Dataset acquisition logged: "
            "Dataset=%s; RunID=%s",
            dataset_id,
            run_id,
        )

        logger.info(
            "Download Dataset execution completed: "
            "Dataset=%s; Bytes=%s",
            dataset_id,
            result.bytes_written,
        )

        return execution.output_path

    except Exception as error:

        if staging_path is not None:

            staging_path.unlink(
                missing_ok=True
            )

        logger.exception(
            "Download Dataset execution failed: "
            "Dataset=%s; Error=%s",
            dataset_id,
            error,
        )

        try:

            notes = build_failure_notes(
                execution,
                error,
            )

            log_dataset_event(
                output_path=(
                    execution.output_path
                ),
                status=FAILED_STATUS,
                records_produced=None,
                notes=notes,
            )

        except Exception as logging_error:

            logger.exception(
                "Unable to persist failed "
                "Download acquisition event: %s",
                logging_error,
            )

        raise


# ============================================================================
# DOWNLOAD ENGINE
# ============================================================================

def extract_download(
    *,
    dataset_id: str | None = None,
    connect_timeout: float = (
        DEFAULT_CONNECT_TIMEOUT_SECONDS
    ),
    read_timeout: float = (
        DEFAULT_READ_TIMEOUT_SECONDS
    ),
    max_attempts: int = (
        DEFAULT_MAX_ATTEMPTS
    ),
    backoff_seconds: float = (
        DEFAULT_BACKOFF_SECONDS
    ),
    chunk_size_bytes: int = (
        DEFAULT_CHUNK_SIZE_BYTES
    ),
    max_download_size_bytes: int = (
        DEFAULT_MAX_DOWNLOAD_SIZE_BYTES
    ),
    user_agent: str = (
        DEFAULT_USER_AGENT
    ),
    fail_fast: bool = False,
) -> list:
    """
    Execute registry-driven Download acquisition.

    When dataset_id is supplied, exactly one active Download Dataset
    is executed.

    Otherwise, all active Download Datasets are executed.

    Independent failures are collected unless fail_fast=True.
    """

    normalized_connect_timeout = (
        positive_number(
            connect_timeout,
            "connect_timeout",
        )
    )

    normalized_read_timeout = (
        positive_number(
            read_timeout,
            "read_timeout",
        )
    )

    normalized_max_attempts = (
        positive_integer(
            max_attempts,
            "max_attempts",
        )
    )

    normalized_backoff_seconds = (
        positive_number(
            backoff_seconds,
            "backoff_seconds",
        )
    )

    normalized_chunk_size = (
        positive_integer(
            chunk_size_bytes,
            "chunk_size_bytes",
        )
    )

    normalized_max_size = (
        positive_integer(
            max_download_size_bytes,
            "max_download_size_bytes",
        )
    )

    normalized_user_agent = (
        optional_text(
            user_agent
        )
    )

    if normalized_user_agent is None:

        raise ValueError(
            "user_agent cannot be empty."
        )

    registry = (
        build_resolved_registry()
    )

    datasets = (
        select_download_datasets(
            registry,
            dataset_id=dataset_id,
        )
    )

    if datasets.empty:

        logger.info(
            "No active Download Datasets "
            "are configured."
        )

        return []

    successful_paths = []
    failures = []

    logger.info(
        "Generic Download acquisition started: "
        "Datasets=%s",
        len(
            datasets
        ),
    )

    with requests.Session() as session:

        for _, row in (
            datasets.iterrows()
        ):

            configuration = (
                row.to_dict()
            )

            current_dataset_id = (
                optional_text(
                    configuration.get(
                        DATASET_KEY
                    )
                )
            )

            try:

                execution = (
                    resolve_dataset_execution(
                        configuration
                    )
                )

                output_path = (
                    execute_download_dataset(
                        execution,
                        session=session,
                        connect_timeout=(
                            normalized_connect_timeout
                        ),
                        read_timeout=(
                            normalized_read_timeout
                        ),
                        max_attempts=(
                            normalized_max_attempts
                        ),
                        backoff_seconds=(
                            normalized_backoff_seconds
                        ),
                        chunk_size_bytes=(
                            normalized_chunk_size
                        ),
                        max_download_size_bytes=(
                            normalized_max_size
                        ),
                        user_agent=(
                            normalized_user_agent
                        ),
                    )
                )

                successful_paths.append(
                    output_path
                )

            except Exception as error:

                failures.append(
                    {
                        "dataset_id":
                            current_dataset_id,
                        "error_type":
                            type(
                                error
                            ).__name__,
                        "error":
                            str(
                                error
                            ),
                    }
                )

                if fail_fast:

                    logger.error(
                        "Download acquisition stopped "
                        "because fail_fast is enabled."
                    )

                    raise

    if failures:

        logger.error(
            "Generic Download acquisition completed "
            "with failures: Success=%s; Failed=%s",
            len(
                successful_paths
            ),
            len(
                failures
            ),
        )

        raise RuntimeError(
            "One or more Download Dataset "
            "acquisitions failed: "
            f"{failures}"
        )

    logger.info(
        "Generic Download acquisition completed "
        "successfully: Datasets=%s",
        len(
            successful_paths
        ),
    )

    return successful_paths


# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Execute the generic Download acquisition engine.
    """

    arguments = parse_arguments()

    paths = extract_download(
        dataset_id=(
            arguments.dataset_id
        ),
        connect_timeout=(
            arguments.connect_timeout
        ),
        read_timeout=(
            arguments.read_timeout
        ),
        max_attempts=(
            arguments.max_attempts
        ),
        backoff_seconds=(
            arguments.backoff_seconds
        ),
        chunk_size_bytes=(
            arguments.chunk_size_bytes
        ),
        max_download_size_bytes=(
            arguments.max_download_size_bytes
        ),
        user_agent=(
            arguments.user_agent
        ),
        fail_fast=(
            arguments.fail_fast
        ),
    )

    logger.info(
        "Download engine published "
        "%s Dataset(s)",
        len(
            paths
        ),
    )


if __name__ == "__main__":
    main()