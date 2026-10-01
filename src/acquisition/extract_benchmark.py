"""
extract_benchmark.py

Acquisition d'un dataset configuré avec la méthode Download.

Responsabilités
---------------
- obtenir une configuration déjà validée et résolue depuis
  le service Source Registry ;
- sélectionner un dataset actif dont la méthode de production
  est Download ;
- télécharger le fichier dans un emplacement temporaire ;
- vérifier la structure minimale du fichier téléchargé ;
- remplacer atomiquement le fichier cible ;
- enregistrer le résultat de l'acquisition dans SQLite.

La validation du Source Registry et la jointure entre les feuilles
Sources et Datasets sont déléguées à :

src.governance.source_registry

Ce module ne lit pas directement le fichier Excel Source Registry
et ne réimplémente pas ses règles de gouvernance.
"""

import argparse
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import requests

from src.acquisition.acquisition_logger import (
    log_acquisition,
)

from src.governance.source_registry import (
    build_dataset_relative_path,
    get_dataset_by_id,
    get_download_enabled_datasets,
)

from src.utils.logger import logger

# ============================================================================
# TECHNICAL CONFIGURATION
# ============================================================================

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_READ_TIMEOUT_SECONDS = 60

DEFAULT_CHUNK_SIZE_BYTES = (
    1024 * 1024
)

MINIMUM_FILE_SIZE_BYTES = 1

DOWNLOAD_URL_COLUMN = "Download URL"
DATASET_ID_COLUMN = "Dataset ID"
OUTPUT_FILE_COLUMN = "Output File Name"
STORAGE_DIRECTORY_COLUMN = "Storage Directory"
PRODUCTION_METHOD_COLUMN = "Production Method"
ACTIVE_COLUMN = "Active"

DOWNLOAD_METHOD = "Download"

SUCCESS_STATUS = "Success"
FAILED_STATUS = "Failed"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": (
        "application/xml,"
        "text/xml,"
        "application/vnd.ms-excel,"
        "application/octet-stream,"
        "*/*"
    ),
}

# ============================================================================
# PROJECT ROOT
# ============================================================================


def get_project_root():
    """
    Détermine la racine du projet depuis
    l'emplacement du présent module.
    """

    root = Path(
        __file__
    ).resolve().parents[2]

    if not (
        root
        / "src"
    ).is_dir():

        raise RuntimeError(
            "Project root could not be resolved "
            f"from {__file__}."
        )

    return root


# ============================================================================
# COMMAND-LINE ARGUMENTS
# ============================================================================


def parse_arguments():
    """
    Lit les arguments facultatifs du script.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Acquire a dataset configured with "
            "Production Method=Download."
        )
    )

    parser.add_argument(
        "--dataset-id",
        default=None,
        help=(
            "Optional Dataset ID. Required only "
            "when several active download datasets "
            "are configured."
        ),
    )

    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=(
            DEFAULT_CONNECT_TIMEOUT_SECONDS
        ),
        help=(
            "HTTP connection timeout in seconds."
        ),
    )

    parser.add_argument(
        "--read-timeout",
        type=int,
        default=(
            DEFAULT_READ_TIMEOUT_SECONDS
        ),
        help=(
            "HTTP response read timeout in seconds."
        ),
    )

    return parser.parse_args()


# ============================================================================
# INPUT VALIDATION
# ============================================================================


def normalize_required_text(
    value,
    field_name,
):
    """
    Normalise une valeur textuelle obligatoire.
    """

    if value is None:

        raise ValueError(
            f"{field_name} is required."
        )

    normalized_value = str(
        value
    ).strip()

    if not normalized_value:

        raise ValueError(
            f"{field_name} cannot be empty."
        )

    return normalized_value


def normalize_positive_integer(
    value,
    field_name,
):
    """
    Vérifie qu'une valeur est un entier positif.
    """

    if isinstance(
        value,
        bool,
    ):

        raise ValueError(
            f"{field_name} must be a "
            "positive integer."
        )

    try:

        normalized_value = int(
            value
        )

    except (
        TypeError,
        ValueError,
    ) as error:

        raise ValueError(
            f"{field_name} must be a "
            "positive integer."
        ) from error

    if normalized_value <= 0:

        raise ValueError(
            f"{field_name} must be "
            "greater than zero."
        )

    return normalized_value


def normalize_download_url(
    value,
):
    """
    Normalise et valide une URL de téléchargement.
    """

    normalized_url = (
        normalize_required_text(
            value,
            DOWNLOAD_URL_COLUMN,
        )
    )

    try:

        parsed_url = urlparse(
            normalized_url
        )

    except ValueError as error:

        raise ValueError(
            "Download URL is invalid."
        ) from error

    if (
        parsed_url.scheme.lower()
        not in {
            "http",
            "https",
        }
        or not parsed_url.netloc
    ):

        raise ValueError(
            "Download URL must be a valid "
            "HTTP or HTTPS URL."
        )

    if any(
        character.isspace()
        for character in normalized_url
    ):

        raise ValueError(
            "Download URL cannot contain "
            "whitespace."
        )

    return normalized_url


# ============================================================================
# DATASET SELECTION
# ============================================================================


def validate_download_configuration(
    configuration,
):
    """
    Vérifie que la configuration résolue peut être
    utilisée pour une acquisition directe.
    """

    required_fields = {
        DATASET_ID_COLUMN,
        OUTPUT_FILE_COLUMN,
        STORAGE_DIRECTORY_COLUMN,
        PRODUCTION_METHOD_COLUMN,
        DOWNLOAD_URL_COLUMN,
        ACTIVE_COLUMN,
    }

    missing_fields = (
        required_fields
        - set(
            configuration
        )
    )

    if missing_fields:

        raise KeyError(
            "Dataset configuration is missing "
            "required fields: "
            f"{sorted(missing_fields)}"
        )

    if not bool(
        configuration[
            ACTIVE_COLUMN
        ]
    ):

        raise ValueError(
            "The selected dataset is inactive."
        )

    production_method = (
        normalize_required_text(
            configuration[
                PRODUCTION_METHOD_COLUMN
            ],
            PRODUCTION_METHOD_COLUMN,
        )
    )

    if (
        production_method.casefold()
        != DOWNLOAD_METHOD.casefold()
    ):

        raise ValueError(
            "The selected dataset is not "
            "configured for direct download."
        )

    normalize_required_text(
        configuration[
            DATASET_ID_COLUMN
        ],
        DATASET_ID_COLUMN,
    )

    normalize_required_text(
        configuration[
            OUTPUT_FILE_COLUMN
        ],
        OUTPUT_FILE_COLUMN,
    )

    normalize_required_text(
        configuration[
            STORAGE_DIRECTORY_COLUMN
        ],
        STORAGE_DIRECTORY_COLUMN,
    )

    normalize_download_url(
        configuration[
            DOWNLOAD_URL_COLUMN
        ]
    )


def select_download_configuration(
    dataset_id=None,
):
    """
    Sélectionne une configuration Download.

    Si dataset_id est absent, exactement un dataset actif
    doit être configuré avec Production Method=Download.
    """

    if dataset_id is not None:

        normalized_dataset_id = (
            normalize_required_text(
                dataset_id,
                "dataset_id",
            )
        )

        configuration = (
            get_dataset_by_id(
                normalized_dataset_id
            )
            .to_dict()
        )

        validate_download_configuration(
            configuration
        )

        return configuration

    downloadable_datasets = (
        get_download_enabled_datasets()
    )

    if downloadable_datasets.empty:

        raise ValueError(
            "No active download dataset was found "
            "in the Source Registry."
        )

    if len(
        downloadable_datasets
    ) != 1:

        dataset_ids = (
            downloadable_datasets[
                DATASET_ID_COLUMN
            ]
            .astype(str)
            .tolist()
        )

        raise ValueError(
            "Several active download datasets "
            "were found. Provide --dataset-id. "
            "Available values: "
            f"{dataset_ids}"
        )

    configuration = (
        downloadable_datasets
        .iloc[0]
        .to_dict()
    )

    validate_download_configuration(
        configuration
    )

    return configuration


# ============================================================================
# OUTPUT PATH
# ============================================================================


def resolve_output_path(
    configuration,
):
    """
    Construit le chemin absolu du fichier cible
    à partir de la configuration résolue.
    """

    relative_path = (
        build_dataset_relative_path(
            configuration
        )
    )

    if (
        relative_path.is_absolute()
        or ".." in relative_path.parts
    ):

        raise ValueError(
            "The configured output path must "
            "remain inside the project."
        )

    root = (
        get_project_root()
        .resolve()
    )

    output_path = (
        root
        / relative_path
    ).resolve()

    try:

        output_path.relative_to(
            root
        )

    except ValueError as error:

        raise ValueError(
            "The resolved output path is "
            "outside the project root."
        ) from error

    return output_path


# ============================================================================
# HTTP SESSION
# ============================================================================


def create_http_session():
    """
    Crée une session HTTP réutilisable.
    """

    session = requests.Session()

    session.headers.update(
        HTTP_HEADERS
    )

    return session


# ============================================================================
# DOWNLOAD
# ============================================================================


def download_to_temporary_file(
    session,
    download_url,
    target_directory,
    timeout,
):
    """
    Télécharge le contenu vers un fichier temporaire.
    """

    temporary_path = None

    try:

        with session.get(
            download_url,
            stream=True,
            allow_redirects=True,
            timeout=timeout,
        ) as response:

            response.raise_for_status()

            with NamedTemporaryFile(
                mode="wb",
                prefix="download_",
                suffix=".tmp",
                dir=target_directory,
                delete=False,
            ) as temporary_file:

                temporary_path = Path(
                    temporary_file.name
                )

                for chunk in response.iter_content(
                    chunk_size=(
                        DEFAULT_CHUNK_SIZE_BYTES
                    )
                ):

                    if chunk:

                        temporary_file.write(
                            chunk
                        )

                temporary_file.flush()

        return temporary_path

    except requests.Timeout as error:

        raise TimeoutError(
            "The download exceeded the "
            "configured timeout."
        ) from error

    except requests.RequestException as error:

        raise RuntimeError(
            "HTTP download failed: "
            f"{error}"
        ) from error

    except Exception:

        if (
            temporary_path is not None
            and temporary_path.exists()
        ):

            temporary_path.unlink()

        raise


# ============================================================================
# FILE VALIDATION
# ============================================================================


def read_file_header(
    file_path,
    size=8192,
):
    """
    Lit le début d'un fichier.
    """

    try:

        with file_path.open(
            mode="rb",
        ) as downloaded_file:

            return downloaded_file.read(
                size
            )

    except OSError as error:

        raise RuntimeError(
            "Unable to read downloaded file: "
            f"{file_path}"
        ) from error


def validate_not_html_response(
    file_path,
):
    """
    Vérifie que le fournisseur n'a pas retourné
    une page HTML à la place du fichier demandé.
    """

    header = (
        read_file_header(
            file_path
        )
        .lstrip()
        .lower()
    )

    if header.startswith(
        (
            b"<!doctype html",
            b"<html",
        )
    ):

        raise ValueError(
            "The provider returned HTML instead "
            "of the expected data file."
        )


def validate_spreadsheetml_structure(
    file_path,
):
    """
    Vérifie les marqueurs structurels minimaux
    d'un workbook SpreadsheetML.

    Le fichier n'est pas soumis à un parseur XML strict,
    car certains fichiers SpreadsheetML exploitables par
    Excel peuvent contenir des caractères non conformes
    aux exigences strictes d'un parseur XML standard.
    
    """

    try:

        content = (
            file_path
            .read_bytes()
            .lower()
        )

    except OSError as error:

        raise RuntimeError(
            "Unable to read downloaded workbook: "
            f"{file_path}"
        ) from error

    workbook_markers = (
        b"<workbook",
        b"<ss:workbook",
        b":workbook",
    )

    worksheet_markers = (
        b"<worksheet",
        b"<ss:worksheet",
        b":worksheet",
    )

    if not any(
        marker in content
        for marker in workbook_markers
    ):

        raise ValueError(
            "The downloaded file has no "
            "SpreadsheetML workbook structure."
        )

    if not any(
        marker in content
        for marker in worksheet_markers
    ):

        raise ValueError(
            "The downloaded workbook contains "
            "no worksheet structure."
        )

    worksheet_counts = [
        content.count(
            marker
        )
        for marker in worksheet_markers
    ]

    return max(
        max(
            worksheet_counts
        ),
        1,
    )


def validate_downloaded_file(
    file_path,
):
    """
    Exécute les contrôles du fichier téléchargé.
    """

    try:

        file_size = (
            file_path
            .stat()
            .st_size
        )

    except OSError as error:

        raise RuntimeError(
            "Unable to inspect downloaded file: "
            f"{file_path}"
        ) from error

    if (
        file_size
        < MINIMUM_FILE_SIZE_BYTES
    ):

        raise ValueError(
            "The downloaded file is empty."
        )

    validate_not_html_response(
        file_path
    )

    worksheet_count = (
        validate_spreadsheetml_structure(
            file_path
        )
    )

    return {
        "file_size_bytes":
            file_size,
        "worksheet_count":
            worksheet_count,
    }


# ============================================================================
# ATOMIC REPLACEMENT
# ============================================================================


def replace_output_file(
    temporary_file,
    output_file,
):
    """
    Remplace le fichier cible après validation.
    """

    try:

        temporary_file.replace(
            output_file
        )

    except PermissionError as error:

        raise PermissionError(
            "The output file is open or locked. "
            "Close it and rerun the acquisition."
        ) from error

    except OSError as error:

        raise RuntimeError(
            "Unable to replace output file: "
            f"{output_file}"
        ) from error


# ============================================================================
# ACQUISITION
# ============================================================================


def extract_benchmark(
    dataset_id=None,
    connect_timeout=(
        DEFAULT_CONNECT_TIMEOUT_SECONDS
    ),
    read_timeout=(
        DEFAULT_READ_TIMEOUT_SECONDS
    ),
):
    """
    Télécharge et valide le dataset sélectionné.

    La configuration reçue a déjà été validée et résolue
    par le service Source Registry.
    """

    normalized_connect_timeout = (
        normalize_positive_integer(
            connect_timeout,
            "connect_timeout",
        )
    )

    normalized_read_timeout = (
        normalize_positive_integer(
            read_timeout,
            "read_timeout",
        )
    )

    configuration = (
        select_download_configuration(
            dataset_id
        )
    )

    download_url = (
        normalize_download_url(
            configuration[
                DOWNLOAD_URL_COLUMN
            ]
        )
    )

    output_path = (
        resolve_output_path(
            configuration
        )
    )

    target_directory = (
        output_path.parent
    )

    temporary_file = None

    logger.info(
        "Download acquisition started "
        "for dataset %s",
        configuration[
            DATASET_ID_COLUMN
        ],
    )

    logger.info(
        "Output file resolved: %s",
        output_path,
    )

    try:

        target_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        with create_http_session() as session:

            temporary_file = (
                download_to_temporary_file(
                    session=session,
                    download_url=download_url,
                    target_directory=(
                        target_directory
                    ),
                    timeout=(
                        normalized_connect_timeout,
                        normalized_read_timeout,
                    ),
                )
            )

        validation_results = (
            validate_downloaded_file(
                temporary_file
            )
        )

        replace_output_file(
            temporary_file,
            output_path,
        )

        temporary_file = None

        run_id = log_acquisition(
            output_file=output_path,
            storage_location=target_directory,
            status=SUCCESS_STATUS,
            records_produced=None,
            notes=(
                "File size bytes="
                f"{validation_results['file_size_bytes']}; "
                "Worksheets="
                f"{validation_results['worksheet_count']}"
            ),
        )

        logger.info(
            "Acquisition Run ID: %s",
            run_id,
        )

        logger.info(
            "Download acquisition completed "
            "successfully"
        )

        return output_path

    except Exception as error:

        logger.exception(
            "Download acquisition failed: %s",
            error,
        )

        try:

            log_acquisition(
                output_file=output_path,
                storage_location=(
                    target_directory
                ),
                status=FAILED_STATUS,
                records_produced=None,
                notes=str(
                    error
                ),
            )

        except Exception as logging_error:

            logger.exception(
                "Unable to persist failed "
                "acquisition: %s",
                logging_error,
            )

        raise

    finally:

        if (
            temporary_file is not None
            and temporary_file.exists()
        ):

            try:

                temporary_file.unlink()

            except OSError as cleanup_error:

                logger.warning(
                    "Unable to remove temporary "
                    "file %s: %s",
                    temporary_file,
                    cleanup_error,
                )


# ============================================================================
# MAIN
# ============================================================================


def main():
    """
    Point d'entrée du script.
    """

    arguments = parse_arguments()

    extract_benchmark(
        dataset_id=(
            arguments.dataset_id
        ),
        connect_timeout=(
            arguments.connect_timeout
        ),
        read_timeout=(
            arguments.read_timeout
        ),
    )


if __name__ == "__main__":

    main()