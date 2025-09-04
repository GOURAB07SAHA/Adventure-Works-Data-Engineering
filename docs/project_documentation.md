# Adventure Works Data Engineering Project Documentation

## Project Overview

This project implements a modern data engineering pipeline using the Adventure Works dataset. It follows the medallion architecture with Bronze, Silver, and Gold layers for data processing and transformation.

## Data Architecture

### Bronze Layer

The Bronze layer contains raw data in CSV format from the Adventure Works dataset. These files include:

- AdventureWorks_Calendar.csv
- AdventureWorks_Customers.csv
- AdventureWorks_Product_Categories.csv
- AdventureWorks_Product_Subcategories.csv
- AdventureWorks_Products.csv
- AdventureWorks_Returns.csv
- AdventureWorks_Sales_2015.csv
- AdventureWorks_Sales_2016.csv
- AdventureWorks_Sales_2017.csv
- AdventureWorks_Territories.csv

### Silver Layer

The Silver layer contains cleaned and transformed data stored in Parquet format. The transformation process includes:

- Data type conversions
- Date formatting
- Null handling
- Combining multiple years of sales data
- Basic data quality checks

### Gold Layer

The Gold layer contains business-ready data views for analytics and reporting. These include:

- Sales Summary: Aggregated sales data by product
- Customer Insights: Customer segmentation and spending patterns
- Monthly Sales: Time series analysis of sales data
- Product Analytics: Product performance metrics

## Project Structure

```
├── Data/                      # Bronze layer data (CSV files)
├── silver/                    # Silver layer data (Parquet files, generated)
├── gold/                      # Gold layer data (Parquet files, generated)
├── docs/                      # Project documentation
├── Reference Script/          # Reference scripts for data processing
├── src/                       # Source code for data processing
│   ├── bronze_to_silver.py    # Script to process bronze to silver layer
│   ├── silver_to_gold.py      # Script to process silver to gold layer
│   ├── main.py                # Main script to run the entire pipeline
│   ├── visualize.py           # Script to create visualizations
│   └── architecture.py        # Script to generate architecture diagram
├── visualizations/            # Generated visualizations (PNG files, generated)
└── requirements.txt           # Python dependencies
```

## Data Processing Workflow

1. **Bronze to Silver Layer Processing**
   - Load raw CSV data from the Bronze layer
   - Clean and transform data (type conversions, date formatting, etc.)
   - Write transformed data to Parquet files in the Silver layer

2. **Silver to Gold Layer Processing**
   - Load cleaned data from the Silver layer
   - Create business-ready views with aggregations and transformations
   - Write views to Parquet files in the Gold layer

3. **Visualization**
   - Create visualizations from Gold layer data
   - Generate reports and dashboards

## Running the Pipeline

### Prerequisites

- Python 3.8+
- PySpark
- Pandas
- Matplotlib
- Seaborn

### Installation

```bash
pip install -r requirements.txt
```

### Running the Full Pipeline

```bash
python src/main.py --layers all
```

### Running Specific Layers

```bash
# Process only bronze to silver layer
python src/main.py --layers bronze_to_silver

# Process only silver to gold layer
python src/main.py --layers silver_to_gold
```

### Generating Visualizations

```bash
python src/visualize.py --visualizations all
```

### Generating Architecture Diagram

```bash
python src/architecture.py
```

## Data Model

### Calendar Table
- Date: Date
- Year: Integer
- Month: Integer
- Day: Integer
- DayOfWeek: Integer
- DayName: String
- MonthName: String
- Quarter: Integer

### Customers Table
- CustomerKey: Integer
- FirstName: String
- LastName: String
- BirthDate: Date
- MaritalStatus: String
- Gender: String
- EmailAddress: String
- AnnualIncome: Double
- TotalChildren: Integer
- Education: String
- Occupation: String

### Products Table
- ProductKey: Integer
- ProductSubcategoryKey: Integer
- ProductName: String
- ModelName: String
- ProductDescription: String
- ProductColor: String
- ProductSize: String
- ProductStyle: String
- StandardCost: Double
- ListPrice: Double
- DealerPrice: Double

### Sales Table
- SalesOrderNumber: String
- SalesOrderLineNumber: Integer
- OrderDate: Date
- StockDate: Date
- ProductKey: Integer
- CustomerKey: Integer
- TerritoryKey: Integer
- OrderQuantity: Integer
- UnitPrice: Double
- SalesAmount: Double

## Future Enhancements

1. **Data Quality Monitoring**
   - Implement data quality checks and monitoring
   - Set up alerts for data quality issues

2. **Incremental Processing**
   - Implement incremental data processing for efficiency
   - Track data lineage and versioning

3. **Advanced Analytics**
   - Implement machine learning models for sales forecasting
   - Customer segmentation and product recommendations

4. **Dashboard Integration**
   - Integrate with BI tools like Power BI or Tableau
   - Create interactive dashboards for business users

5. **Automated Pipeline**
   - Set up automated pipeline execution with scheduling
   - Implement CI/CD for data pipeline code