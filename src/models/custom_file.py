#!/usr/bin/env python3
"""
Daily Meowfacts Extractor
Fetches all meowfacts from the Meowfacts API across all supported languages
and outputs a single JSON dataset.
"""

import json
import uuid
import requests
from datetime import datetime

API_URL = "https://meowfacts.herokuapp.com/"

# Supported languages extracted from API docs & repo
SUPPORTED_LANGUAGES = [
    "eng", "ces", "ger", "ben", "esp", "rus", "por",
    "fil", "ukr", "urd", "ita", "zho", "kor"
]


def get_timestamp() -> str:
    """Return current processing timestamp in ISO format."""
    return datetime.utcnow().isoformat() + "Z"


def fetch_meowfacts(lang: str, count: int = 200):
    """
    Fetch multiple meowfacts for a given language.
    :param lang: ISO 639-2 code
    :param count: number of facts to request
    :return: list of dicts with language and fact
    """
    params = {"lang": lang, "count": count}
    response = requests.get(API_URL, params=params)
    response.raise_for_status()

    data = response.json()
    facts = data.get("data", [])

    # Add event_id and processing_timestamp
    processed_timestamp = get_timestamp()

    return [
        {
            "event_id": str(uuid.uuid4()),
            "processing_timestamp": processed_timestamp,
            "language": lang,
            "fact": fact
        }
        for fact in facts
    ]


def main():
    all_facts = []

    print("🚀 Starting Meowfacts extraction...\n")

    for lang in SUPPORTED_LANGUAGES:
        print(f"🔹 Fetching facts for language: {lang}")
        try:
            facts = fetch_meowfacts(lang)
            all_facts.extend(facts)
        except Exception as e:
            print(f" Error fetching {lang}: {e}")

    output_file = "requested_dataset.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_facts, f, indent=2, ensure_ascii=False)


    print(f"\n File saved: {output_file}")
    print(f" Total rows: {len(all_facts)}")


if __name__ == "__main__":
    main()
