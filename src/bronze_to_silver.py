#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bronze to Silver Layer Transformation

This script processes data from the bronze layer (CSV files) to the silver layer (cleaned Parquet files)
using pandas instead of PySpark.
"""

import os
import pandas as pd
import numpy as np


def process_calendar_data(data_dir, silver_dir):
    """
    Process calendar data from CSV to Parquet
    """
    # Read calendar data
    calendar_path = os.path.join(data_dir, "AdventureWorks_Calendar.csv")
    calendar_df = pd.read_csv(calendar_path, encoding='latin1')
    
    # Clean and transform data
    calendar_df['Date'] = pd.to_datetime(calendar_df['Date'])
    calendar_df['Year'] = calendar_df['Date'].dt.year
    calendar_df['Month'] = calendar_df['Date'].dt.month
    calendar_df['Day'] = calendar_df['Date'].dt.day
    calendar_df['DayOfWeek'] = calendar_df['Date'].dt.dayofweek
    calendar_df['DayName'] = calendar_df['Date'].dt.day_name()
    calendar_df['MonthName'] = calendar_df['Date'].dt.month_name()
    calendar_df['Quarter'] = calendar_df['Date'].dt.quarter
    
    # Write to silver layer
    output_path = os.path.join(silver_dir, "AdventureWorks_Calendar.parquet")
    calendar_df.to_parquet(output_path, index=False)
    
    print(f"Calendar data processed: {calendar_df.shape[0]} rows")
    return calendar_df


def process_customer_data(data_dir, silver_dir):
    """
    Process customer data from CSV to Parquet
    """
    # Read customer data
    customer_path = os.path.join(data_dir, "AdventureWorks_Customers.csv")
    customer_df = pd.read_csv(customer_path, encoding='latin1')
    
    # Clean and transform data
    customer_df['CustomerKey'] = customer_df['CustomerKey'].astype(int)
    
    # Handle missing values
    customer_df['Gender'] = customer_df['Gender'].fillna('Unknown')
    customer_df['MaritalStatus'] = customer_df['MaritalStatus'].fillna('Unknown')
    
    # Write to silver layer
    output_path = os.path.join(silver_dir, "AdventureWorks_Customers.parquet")
    customer_df.to_parquet(output_path, index=False)
    
    print(f"Customer data processed: {customer_df.shape[0]} rows")
    return customer_df


def process_product_data(data_dir, silver_dir):
    """
    Process product data from CSV to Parquet
    """
    # Read product data
    product_path = os.path.join(data_dir, "AdventureWorks_Products.csv")
    product_df = pd.read_csv(product_path, encoding='latin1')
    
    # Clean and transform data
    product_df['ProductKey'] = product_df['ProductKey'].astype(int)
    
    # Handle missing values
    product_df['ProductName'] = product_df['ProductName'].fillna('Unknown Product')
    product_df['ModelName'] = product_df['ModelName'].fillna('Unknown Model')
    product_df['ProductSubcategoryKey'] = product_df['ProductSubcategoryKey'].fillna(-1)
    
    # Write to silver layer
    output_path = os.path.join(silver_dir, "AdventureWorks_Products.parquet")
    product_df.to_parquet(output_path, index=False)
    
    print(f"Product data processed: {product_df.shape[0]} rows")
    return product_df


def process_sales_data(data_dir, silver_dir):
    """
    Process sales data from CSV to Parquet
    """
    # Read sales data from multiple files
    sales_files = [
        "AdventureWorks_Sales_2015.csv",
        "AdventureWorks_Sales_2016.csv",
        "AdventureWorks_Sales_2017.csv"
    ]
    
    # Combine all sales data
    all_sales_dfs = []
    for file in sales_files:
        file_path = os.path.join(data_dir, file)
        if os.path.exists(file_path):
            print(f"Processing {file}...")
            sales_df = pd.read_csv(file_path, encoding='latin1')
            all_sales_dfs.append(sales_df)
    
    # Concatenate all sales dataframes
    if all_sales_dfs:
        sales_df = pd.concat(all_sales_dfs, ignore_index=True)
        
        # Clean and transform data
        sales_df['OrderDate'] = pd.to_datetime(sales_df['OrderDate'])
        sales_df['StockDate'] = pd.to_datetime(sales_df['StockDate'])
        sales_df['OrderQuantity'] = sales_df['OrderQuantity'].astype(int)
        sales_df['CustomerKey'] = sales_df['CustomerKey'].astype(int)
        sales_df['ProductKey'] = sales_df['ProductKey'].astype(int)
        
        # Add calculated fields for silver layer
        # Since we don't have SalesAmount and TotalProductCost in the original data,
        # we'll create them based on available data
        
        # Get product data for pricing information
        product_path = os.path.join(data_dir, "AdventureWorks_Products.csv")
        product_df = pd.read_csv(product_path, encoding='latin1')
        
        # Merge with product data to get pricing
        merged_df = sales_df.merge(product_df[['ProductKey', 'ProductPrice']], on='ProductKey', how='left')
        
        # Calculate sales amount and cost
        merged_df['UnitPrice'] = merged_df['ProductPrice']
        merged_df['SalesAmount'] = merged_df['OrderQuantity'] * merged_df['UnitPrice']
        merged_df['TotalProductCost'] = merged_df['SalesAmount'] * 0.7  # Assuming 30% profit margin
        merged_df['Profit'] = merged_df['SalesAmount'] - merged_df['TotalProductCost']
        merged_df['ProfitMargin'] = merged_df['Profit'] / merged_df['SalesAmount']
        
        # Select final columns for silver layer
        final_cols = [
            'OrderDate', 'StockDate', 'OrderNumber', 'ProductKey', 'CustomerKey',
            'TerritoryKey', 'OrderLineItem', 'OrderQuantity', 'UnitPrice',
            'SalesAmount', 'TotalProductCost', 'Profit', 'ProfitMargin'
        ]
        
        # Filter to only needed columns
        final_df = merged_df[final_cols]
        
        # Write to silver layer
        output_path = os.path.join(silver_dir, "AdventureWorks_Sales.parquet")
        final_df.to_parquet(output_path, index=False)
        
        print(f"Sales data processed: {final_df.shape[0]} rows")
        return final_df
    else:
        print("No sales data found")
        return None


def main():
    """
    Main function to process all bronze data to silver
    """
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "Data")
    silver_dir = os.path.join(base_dir, "silver")
    
    # Create silver directory if it doesn't exist
    os.makedirs(silver_dir, exist_ok=True)
    
    # Process all data sources
    process_calendar_data(data_dir, silver_dir)
    process_customer_data(data_dir, silver_dir)
    process_product_data(data_dir, silver_dir)
    process_sales_data(data_dir, silver_dir)
    
    print("All bronze data processed to silver layer successfully")


if __name__ == "__main__":
    main()