# Flight Delay & Weather Database

**Course:** Databases 1 (2025-2026) – Master 1
**Authors:** Ayman EL ALASS & Abderaouf KHELFAOUI

## Description

This project models and creates a relational database between US flight delays and weather conditions. It includes scripts to parse raw CSV data, populate the database, and run analytical queries.

## Data Sources

The raw data files are not included in this repository due to their size. Please download them from Kaggle and place them in the root folder:

1.  **Flights:** [Kaggle Flight Delays](https://www.kaggle.com/datasets/usdot/flight-delays)
2.  **Weather:** [Kaggle Historical Hourly Weather](https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data)

## Installation

Here is the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

To generate the database and execute the queries, run:

```bash
python main.py
```

## Project Contents

  * `main.py`: Main script for database creation and querying.
  * `report/`: Contains the project report and presentation slides.