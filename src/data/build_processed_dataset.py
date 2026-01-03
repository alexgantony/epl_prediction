import re
from pathlib import Path
from typing import TextIO

import pandas as pd

raw_data_path = Path("data") / "raw"
raw_data_list = [file for file in raw_data_path.iterdir()]

processed_data_path = Path("data") / "processed" / "epl_matches_process.csv"

CORE_IDENTIFIERS = [
    "Date",
    "HomeTeam",
    "AwayTeam",
]

CORE_PREMATCH_FEATURES = [
    "AvgH",
    "AvgD",
    "AvgA",
]

CORE_TARGETS = [
    "FTHG",
    "FTAG",
    "FTR",
]

REQUIRED_CORE_FEATURES = CORE_IDENTIFIERS + CORE_PREMATCH_FEATURES + CORE_TARGETS

OPTIONAL_DIAGNOSTIC_FEATURES = [
    # halftime info
    "HTHG",
    "HTAG",
    "HTR",
    # shots
    "HS",
    "AS",
    "HST",
    "AST",
    # set pieces & fouls
    "HC",
    "AC",
    "HF",
    "AF",
    # discipline
    "HY",
    "AY",
    "HR",
    "AR",
]

PROCESSED_DATASET_COLUMNS = [
    # identifiers
    "Date",
    "Season",
    "HomeTeam",
    "AwayTeam",
    # pre-match market features (core signal)
    "AvgH",
    "AvgD",
    "AvgA",
    # targets
    "FTHG",
    "FTAG",
    "FTR",
    # post-match diagnostic features
    "HTHG",
    "HTAG",
    "HTR",
    "HS",
    "AS",
    "HST",
    "AST",
    "HC",
    "AC",
    "HF",
    "AF",
    "HY",
    "AY",
    "HR",
    "AR",
]


def finalize_schema(file_obj: TextIO, season: Path) -> pd.DataFrame:
    """
     Docstring for apply_finalized_schema
     Normalize a raw season CSV into a schema-consistent DataFrame.

    Guarantees:
        - Required core features must exist
        - Missing optional diagnostics are filled with NA
        - Season column is injected as YYYY_YY
        - Output schema and order are deterministic

     :param file_obj: Open file-like object for a single season CSV.
     :type file_obj: TextIO
     :param season:  Path to the season file. The filename is used to derive the season label.
     :type season: Path

     → returns:
        pandas.DataFrame
        DataFrame with:
        - exact columns defined in PROCESSED_DATASET_COLUMNS
        - deterministic column order
        - injected Season column in the format: YYYY_YY
          (example: 2014_15)
    """

    file_obj.seek(0)

    df = pd.read_csv(file_obj)

    headers = list(df.columns)

    # Check for core features
    if not (set(REQUIRED_CORE_FEATURES).issubset(headers)):
        raise ValueError("Required features doesn't exist in the dataset.")

    # check for optional diagnostic features
    if not (set(OPTIONAL_DIAGNOSTIC_FEATURES).issubset(headers)):
        print("Warning: Post-match diagnostic features missing")

    # add "Season" feature
    yr = str(re.findall(r"\d+", season.name)[0])
    season_value = f"20{yr[0:2]}_{yr[2:]}"
    df["Season"] = season_value

    missing_optional = set(OPTIONAL_DIAGNOSTIC_FEATURES) - set(headers)

    for col in missing_optional:
        df[col] = pd.NA

    validated_df = df[PROCESSED_DATASET_COLUMNS]

    return validated_df


# Collect each season's DataFrame first, then merge them once at the end.
frames: list[pd.DataFrame] = []

for season_file in raw_data_list:
    with open(season_file, "r", encoding="utf-8") as csv_file:
        try:
            processed_df = finalize_schema(csv_file, season_file)
            frames.append(processed_df)
        except ValueError as e:
            print(f"Skipping {season_file.name}: {e}")

final_df = pd.concat(frames, ignore_index=True)

final_df.to_csv(processed_data_path, index=False)
print(f"{processed_data_path}: Created successfully.")
