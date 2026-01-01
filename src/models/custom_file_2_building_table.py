import json
import pandas as pd

INPUT_FILE = "requested_dataset.json"   # результат твоего скрипта
CSV_FILE = "table_dataset.csv"
EXCEL_FILE = "table_dataset.xlsx"


def build_table():
    # Load JSON into Python
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Convert list of dicts → DataFrame
    df = pd.DataFrame(data)

    print(" Table preview:")
    print(df.head(), "\n")

    print(f"Total rows: {len(df)}")

    # Save to different formats
    df.to_csv(CSV_FILE, index=False)
    df.to_excel(EXCEL_FILE, index=False)


    print("\n Saved:")
    print(f"• CSV → {CSV_FILE}")
    print(f"• Excel → {EXCEL_FILE}")


if __name__ == "__main__":
    build_table()
