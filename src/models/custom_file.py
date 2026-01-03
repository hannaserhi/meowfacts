#!/usr/bin/env python3
"""
File_name: custom_file.py
Description: Fetches all facts from the Meowfacts API across all supported languages
and outputs a single JSON dataset.
"""

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration constants
API_URL = "https://meowfacts.herokuapp.com/"
API_TIMEOUT = 30  # seconds
DEFAULT_FACT_COUNT = 200
OUTPUT_FILE = "requested_dataset.json"

# Supported languages extracted from API docs & repo
SUPPORTED_LANGUAGES = [
    "eng", "ces", "ger", "ben", "esp", "rus", "por",
    "fil", "ukr", "urd", "ita", "zho", "kor"
]


def get_timestamp() -> str:
    """Return current processing timestamp in ISO format."""
    return datetime.utcnow().isoformat() + "Z"


def fetch_meowfacts(lang: str, count: int = DEFAULT_FACT_COUNT) -> List[Dict[str, Any]]:
    """
    Fetch multiple meowfacts for a given language.

    :param lang: ISO 639-2 code
    :param count: number of facts to request
    :return: list of dicts with language and fact
    :raises: RequestException, ValueError
    """
    if not lang or not isinstance(lang, str):
        raise ValueError(f"Invalid language code: {lang}")

    if count <= 0 or count > 1000:
        raise ValueError(f"Count must be between 1 and 1000, got: {count}")

    try:
        params = {"lang": lang, "count": count}
        logger.debug(f"Fetching facts for language '{lang}' with count={count}")

        response = requests.get(API_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()

        data = response.json()
        facts = data.get("data", [])

        if not isinstance(facts, list):
            logger.warning(f"Expected list of facts for language '{lang}', got {type(facts)}")
            facts = []

        processed_timestamp = get_timestamp()

        result = [
            {
                "event_id": str(uuid.uuid4()),
                "processing_timestamp": processed_timestamp,
                "language": lang,
                "fact": str(fact)  # Ensure fact is a string
            }
            for fact in facts
        ]

        logger.info(f"Successfully fetched {len(result)} facts for language '{lang}'")
        return result

    except Timeout:
        logger.error(f"Timeout while fetching facts for language '{lang}'")
        raise
    except ConnectionError as e:
        logger.error(f"Connection error while fetching facts for language '{lang}': {e}")
        raise
    except HTTPError as e:
        logger.error(f"HTTP error while fetching facts for language '{lang}': {e} (Status: {response.status_code})")
        raise
    except (KeyError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing response for language '{lang}': {e}")
        raise ValueError(f"Invalid API response format for language '{lang}'") from e
    except RequestException as e:
        logger.error(f"Request exception while fetching facts for language '{lang}': {e}")
        raise


def write_output_file(data: List[Dict[str, Any]], output_path: str) -> None:
    """
    Write data to JSON file with error handling.

    :param data: List of dictionaries to write
    :param output_path: Path to output file
    :raises: IOError, OSError
    """
    if not data:
        logger.warning("No data to write to output file")

    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Successfully wrote {len(data)} records to {output_path}")

    except IOError as e:
        logger.error(f"IO error while writing output file '{output_path}': {e}")
        raise
    except OSError as e:
        logger.error(f"OS error while writing output file '{output_path}': {e}")
        raise


def main() -> None:
    """
    Main entry point for the script.

    :raises: ValueError, IOError, OSError
    """
    all_facts = []
    failed_languages = []

    logger.info("Starting Meowfacts extraction")

    for lang in SUPPORTED_LANGUAGES:
        try:
            facts = fetch_meowfacts(lang)
            all_facts.extend(facts)
        except (RequestException, ValueError) as e:
            logger.error(f"Failed to fetch facts for language '{lang}': {e}")
            failed_languages.append(lang)
            continue
        except Exception as e:
            logger.exception(f"Unexpected error while fetching facts for language '{lang}': {e}")
            failed_languages.append(lang)
            continue

    if not all_facts:
        raise ValueError("No facts were successfully fetched from any language")

    write_output_file(all_facts, OUTPUT_FILE)

    logger.info(f"Extraction complete. Total rows: {len(all_facts)}")
    if failed_languages:
        logger.warning(f"Failed to fetch facts for {len(failed_languages)} language(s): {', '.join(failed_languages)}")


if __name__ == "__main__":
    main()