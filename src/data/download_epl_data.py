from pathlib import Path
from typing import Optional

import requests

url = "https://www.football-data.co.uk/mmz4281/"
start_season_year = 2014
last_season_end_year = 2025
league_code = "/E0"  # Code for premier leauge = "E0"

data_folder = Path("data/raw/")
data_folder.mkdir(parents=True, exist_ok=True)


def validate_csv_content(content: bytes) -> tuple[bool, Optional[str]]:
    """
    Docstring for validate_csv_content

    :param content:
                The raw file content downloaded from the source URL.
                This is the CSV file exactly as received from the server.
    :type content: bytes
    :return:
            A tuple indicating validation result.
            - (True, None) if the content passes all validation checks.
            - (False, str) if the content is invalid, where the string describes the reason for failure
    :rtype: tuple[bool, Optional[str]]
    """

    # Extract the header row only
    header = content.split(b"\n", 1)[0]

    # Inspect only the first 1 KB
    snippet = content[:1000].lower()

    # Count rows using newline
    # Expects: ~380 rows; 20 teams, 38 games in total, (38 x 20) / 20 = 380)
    row_count = content.count(b"\n")

    if b"<html" in snippet or b"<!doctype" in snippet or b"<head" in snippet:
        return (False, "Error: HTML found")

    if len(content) < 5_000:
        return (False, "Error: File size less than 5kb")

    if b"HomeTeam" not in header or b"AwayTeam" not in header:
        return (False, "Error: Header Missing")

    if row_count < 300 or row_count > 420:
        return (False, "Error: Row count outside expected Premier League range")

    # All validation checks passed
    return (True, None)


for i in range(start_season_year, last_season_end_year):
    season_year_format = (
        str(i)[-2:] + str(i + 1)[-2:]  # changing "2014 - 2015" to "1415"
    )
    csv_url = url + season_year_format + league_code + ".csv"

    try:
        response = requests.get(
            csv_url,
            timeout=(5, 20),  # connection timeout = 5secs; read timeout = 20secs
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_error:
        print(f"Season {i}-{i + 1} | HTTPError | {http_error}")
        continue
    except requests.exceptions.ReadTimeout as timeout_error:
        print(f"Season {i}-{i + 1} | ReadTimeout | {timeout_error}")
        continue
    except requests.exceptions.ConnectionError as conn_error:
        print(f"Season {i}-{i + 1} | ConnectionError | {conn_error}")
        continue
    except requests.exceptions.RequestException as request_error:
        print(f"Season {i}-{i + 1} | RequestException | {request_error}")
        continue

    is_valid, validation_error = validate_csv_content(response.content)

    if is_valid:
        csv_file_name = "season_" + season_year_format + ".csv"
        file_to_create = data_folder / csv_file_name

        if file_to_create.exists():
            print(f"Season {i}-{i + 1} | Skipped | File already exists")
            continue
        with open(file_to_create, "wb") as f:
            f.write(response.content)
        print(f"{csv_file_name} downloaded successfully!")
    else:
        print(f"Season {i}-{i + 1} | ValidationError | {validation_error}")
