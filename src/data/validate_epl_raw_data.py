import csv
import json
from datetime import datetime
from pathlib import Path
from pprint import pprint

raw_data_path = Path("data") / "raw"

# Iterate over all raw season CSVs produced by the download step
raw_data_csv_list = [file for file in raw_data_path.iterdir()]

report_path = Path("logs") / "validation_report.json"

RAW_REQUIRED_CORE_COLUMNS = [
    # identifiers
    "Date",
    "HomeTeam",
    "AwayTeam",
    # pre-match odds (core signal)
    "AvgH",
    "AvgD",
    "AvgA",
    # targets
    "FTHG",
    "FTAG",
    "FTR",
]

RAW_OPTIONAL_DIAGNOSTIC_COLUMNS = [
    # halftime info
    "HTHG",
    "HTAG",
    "HTR",
    # shots
    "HS",
    "AS",
    "HST",
    "AST",
    "HHW",
    "AHW",
    # set pieces & fouls
    "HC",
    "AC",
    "HF",
    "AF",
    "HFKC",
    "AFKC",
    # offsides
    "HO",
    "AO",
    # discipline
    "HY",
    "AY",
    "HR",
    "AR",
    "HBP",
    "ABP",
]


issues = []

run_metadata = {
    "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_files_checked": 0,
    "files_passed": 0,
    "files_failed": 0,
}


def log_issue(
    severity: str,
    season: str,
    file: str,
    issue_type: str,
    column: str | None,
    message: str = "",
):
    issues.append(
        {
            "severity": severity,
            "season": season,
            "file": file,
            "issue_type": issue_type,
            "column": column,
            "expected": True,
            "actual": False,
            "message": message,
        }
    )


# Validate each season file independently and collect issues for reporting
for season_file in raw_data_csv_list:
    file_has_error = False
    run_metadata["total_files_checked"] += 1

    # Derive season label (e.g. 2014_15) from filename convention
    season_yr = str(season_file.stem).split("_")[1]
    start_yr = 2000 + int(season_yr[:2])
    end_year = season_yr[-2:]

    season = f"{start_yr}_{end_year}"
    file = season_file.name

    with open(season_file, "r", encoding="utf-8-sig") as csv_file:
        # read schema
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)

        # header normalization
        header = [col.strip() for col in header]

        # schema presence check
        # Core columns are required for model training; missing any is a blocking error
        for req_col in RAW_REQUIRED_CORE_COLUMNS:
            if req_col not in header:
                log_issue(
                    "ERROR",
                    season,
                    file,
                    issue_type="missing_required_column",
                    column=req_col,
                    message="Required column is missing",
                )
                file_has_error = True

        # Optional columns are used only for post-match diagnostics; missing is non-blocking
        for req_col in RAW_OPTIONAL_DIAGNOSTIC_COLUMNS:
            if req_col not in header:
                log_issue(
                    "WARNING",
                    season,
                    file,
                    issue_type="missing_optional_diagnostic_column",
                    column=req_col,
                    message="Optional column for post-match diagnosis is missing",
                )

        # Sanity check: Premier League seasons should have ~380 matches
        for _ in csv_reader:
            pass

        data_rows = csv_reader.line_num - 1
        if not (380 <= data_rows <= 420):
            log_issue(
                "INFO",
                season,
                file,
                issue_type="row_count_issue",
                column=None,
                message=f"Row count outside expected range (380–420 matches); observed: {data_rows}",
            )

        if file_has_error:
            run_metadata["files_failed"] += 1
        else:
            run_metadata["files_passed"] += 1


# Block downstream build step if any ERROR-level issues exist
error_exists = any(issue["severity"] == "ERROR" for issue in issues)
if error_exists:
    print(
        f"\nValidation failed: {run_metadata['files_failed']}/{run_metadata['total_files_checked']} "
        f"files contain ERROR-level issues. Build step blocked.\n"
    )
else:
    print("\nValidation passed. Proceed to build processed dataset.\n")

pprint(run_metadata)

report = {"run_metadata": run_metadata, "issues": issues}

# create a structured validation output in JSON for reporting and auditing
with open(report_path, "w") as outfile:
    json.dump(report, outfile, indent=4)
