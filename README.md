# Adventure Works Data Engineering Project

## Overview
This project implements a modern data engineering pipeline using the Adventure Works dataset. It follows the medallion architecture with Bronze, Silver, and Gold layers for data processing and transformation.

## Project Structure
- **Data/**: Contains raw CSV data files (Bronze layer)
- **Reference Script/**: Contains reference scripts for Silver and Gold layer transformations
- **src/**: Contains source code for data processing

## Data Architecture

### Bronze Layer
Raw data in CSV format from Adventure Works dataset.

### Silver Layer
Cleaned and transformed data stored in Parquet format.

### Gold Layer
Business-ready data views for analytics and reporting.

## Getting Started

### Prerequisites
- Python 3.8+
- PySpark
- Pandas

### Setup
1. Clone this repository
2. Install required packages: `pip install -r requirements.txt`
3. Run the data processing scripts in the `src` directory

## Data Processing
The data processing workflow includes:
1. Loading raw CSV data from the Bronze layer
2. Cleaning and transforming data in the Silver layer
3. Creating business views in the Gold layer

## License
This project is licensed under the MIT License - see the LICENSE file for details.