from pathlib import Path
import re
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "01_raw"
    / "benchmark"
    / "benchmark_holdings.xls"
)

print("Reading benchmark file...")

with open(INPUT_FILE, "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

# --------------------------------------------------
# Extract Holdings worksheet
# --------------------------------------------------

match = re.search(
    r'<ss:Worksheet ss:Name="Holdings">(.*?)</ss:Worksheet>',
    content,
    re.DOTALL
)

if not match:
    raise ValueError("Holdings worksheet not found.")

holdings_content = match.group(1)

# --------------------------------------------------
# Extract all values
# --------------------------------------------------

values = re.findall(
    r'<ss:Data[^>]*>(.*?)</ss:Data>',
    holdings_content,
    re.DOTALL
)

# --------------------------------------------------
# Locate header
# --------------------------------------------------

header = [
    "Ticker",
    "Name",
    "Sector",
    "Asset Class",
    "Market Value",
    "Weight (%)",
    "Notional Value",
    "Quantity",
    "Price",
    "Location",
    "Exchange",
    "Currency",
    "FX Rate",
    "Accrual Date"
]

header_index = None

for i in range(len(values)):
    if values[i:i + len(header)] == header:
        header_index = i
        break

if header_index is None:
    raise ValueError("Header not found.")

print(f"Header found at position {header_index}")

# --------------------------------------------------
# Extract rows
# --------------------------------------------------

data_values = values[
    header_index + len(header):
]

records = []

for i in range(0, len(data_values), len(header)):
    row = data_values[i:i + len(header)]

    if len(row) != len(header):
        continue

    records.append(row)

df = pd.DataFrame(
    records,
    columns=header
)

print("\nDataFrame created.")

print(f"Rows    : {len(df):,}")
print(f"Columns : {len(df.columns)}")

print("\nPreview:\n")

print(
    df[
        ["Ticker", "Name", "Asset Class"]
    ].head(20)
)

# --------------------------------------------------
# Keep Equity only
# --------------------------------------------------

equities = df[
    df["Asset Class"] == "Equity"
].copy()

print(f"\nEquities retained: {len(equities):,}")

# --------------------------------------------------
# Select Security Master attributes
# --------------------------------------------------

equities = equities[
    [
        "Ticker",
        "Name",
        "Location",
        "Exchange",
        "Currency",
        "Asset Class"
    ]
]

# --------------------------------------------------
# Remove duplicates
# --------------------------------------------------

equities = equities.drop_duplicates(
    subset=[
        "Ticker",
        "Exchange"
    ]
)

print(
    f"Unique securities: {len(equities):,}"
)

# --------------------------------------------------
# Export
# --------------------------------------------------

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "01_raw"
    / "securities"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

OUTPUT_FILE = (
    OUTPUT_DIR
    / "security_candidates.csv"
)

equities.to_csv(
    OUTPUT_FILE,
    index=False
)

print(
    f"\nSecurity candidates exported:"
)

print(OUTPUT_FILE)

print("\nPreview:\n")

print(
    equities.head(20)
)