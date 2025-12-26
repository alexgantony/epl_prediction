import json
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
validation_log_path = PROJECT_ROOT / "logs" / "validation_report.json"
validation_summary_path = PROJECT_ROOT / "logs" / "validation_summary.md"

with open(validation_log_path, "r") as json_file:
    report = json.load(json_file)


# Load validation output produced by the raw data validation step
metadata = report["run_metadata"]
issues = report["issues"]

# Convert issue list into DataFrame
df = pd.DataFrame(issues)


with open(validation_summary_path, "a") as summary_file:
    summary_file.write("# EPL Raw Data Validation Summary\n\n## Run Info\n\n")

    # add metadata summary
    metadata_lines = [
        f"- Run timestamp: {metadata['run_timestamp']}\n",
        f"- Files checked: {metadata['total_files_checked']}\n",
        f"- Files passed: {metadata['files_passed']}\n",
        f"- Files failed: {metadata['files_failed']}\n\n",
    ]
    summary_file.writelines(metadata_lines)

    # Validation result
    summary_file.write("## Validation Result\n\n")

    # If any ERROR-level issues exist, the pipeline must stop
    if (df["severity"] == "ERROR").any():
        summary_file.write(
            "**Validation FAILED**\nBuild step blocked due to ERROR-level issues.\n\n"
        )
    else:
        summary_file.write(
            "**Validation PASSED**.\nProceed to build processed dataset.\n\n"
        )

    # Issue Breakdown
    summary_file.write("## Issue Breakdown\n\n")

    # High-level severity distribution across all validation issues
    issue_breakdown = df["severity"].value_counts()
    issue_lines = [
        f"- ERROR: {issue_breakdown.get('ERROR', 0)}\n",
        f"- WARNING: {issue_breakdown.get('WARNING', 0)}\n",
        f"- INFO: {issue_breakdown.get('INFO', 0)}\n\n",
    ]

    summary_file.writelines(issue_lines)

    # Top Failure Reasons

    # Aggregate issue counts by type and severity for root-cause analysis
    issue_counts = df.groupby(by=["issue_type", "severity"]).size().to_frame("count")

    # Separate blocking (ERROR) issues from non-blocking (WARNING / INFO) issues
    blocking_failures = issue_counts.loc[
        issue_counts.index.get_level_values("severity") == "ERROR"
    ].reset_index()
    non_blocking_failures = issue_counts.loc[
        issue_counts.index.get_level_values("severity").isin(["WARNING", "INFO"])
    ].reset_index()

    summary_file.write("## Top Failure Reasons\n\n")
    if blocking_failures.empty:
        summary_file.write(
            "### Blocking Failure Reasons (ERROR)\n\n_No issues found._\n\n"
        )
    else:
        summary_file.write("### Blocking Failure Reasons (ERROR)\n\n")
        for _, row in blocking_failures.iterrows():
            issue_label = str(row["issue_type"]).replace("_", " ").title()
            summary_file.write(f"- {issue_label}: {row['count']}\n")
        summary_file.write("\n")

    if non_blocking_failures.empty:
        summary_file.write(
            "### Non-Blocking Issues Reasons (WARNING | INFO)\n\n_No issues found._\n\n"
        )
    else:
        summary_file.write("### Non-Blocking Issues (WARNING | INFO)\n\n")
        for _, row in non_blocking_failures.iterrows():
            issue_label = str(row["issue_type"]).replace("_", " ").title()
            summary_file.write(f"- {issue_label}: {row['count']}\n")


print("Validation summary generated.")
