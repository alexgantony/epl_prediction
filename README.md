# Generative Probabilistic Forecasting and Simulation System for the English Premier League

---

## Project Overview

---

## System Architecture

---

## Problem Statement

---

## Objectives & Success Metrics

---

## Data Sources

### Raw match data

Source: [Football-Data.co.uk](https://www.football-data.co.uk/englandm.php)  
League: English Premier League  
Seasons: 2014-15 to 2024-25

Raw CSV files are not commited to version control.

To download raw data:

`python src/data/download_epl_raw_data.py`

### Processed Data

Processed datasets are created from raw match CSV files using a strict validation and normalization pipeline.

**Processing steps include:**

- Normalizing header columns by stripping blank lines and inconsistencies

- Verifying the presence of required core columns (missing core columns are treated as errors)

- Checking optional columns and reporting missing fields as warnings

- Validating column counts to detect malformed files

- Adding an explicit season column to each dataset

- Iteratively combining season-wise files into a single consolidated dataset

The processed dataset is written to:
`data/processed/epl_matches_processed.csv`

Processed data file are not committed to version control and are fully reproducible from raw inputs.

To generate the processed dataset:
`python src/data/process_epl_data.py`

---

## Feature Engineering

---

## Modeling Approach

---

## Why Not Traditional ML Models?

---

## Evaluation Methodology

---

## Results & Output

---

## Season Simulation

---

## Assumptions & Limitations

---

## Project Structure

---

## How to Reproduce Results

---

## Future Improvements

---

## Key Learning

---
