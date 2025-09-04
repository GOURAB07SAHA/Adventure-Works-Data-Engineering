#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Silver to Gold Layer Transformation

This script processes data from the silver layer (Parquet files) to the gold layer (business-ready views)
with aggregations and business logic using pandas instead of PySpark.
"""

import os
import pandas as pd
import numpy as np


def create_sales_summary(silver_dir, gold_dir):
    """
    Create sales summary view in gold layer
    """
    # Read sales data from silver
    sales_path = os.path.join(silver_dir, "AdventureWorks_Sales.parquet")
    sales_df = pd.read_parquet(sales_path)
    
    # Read products data from silver
    products_path = os.path.join(silver_dir, "AdventureWorks_Products.parquet")
    products_df = pd.read_parquet(products_path)
    
    # Join sales and products
    joined_df = sales_df.merge(products_df, on='ProductKey')
    
    # Create sales summary
    sales_summary = joined_df.groupby(['ProductKey', 'ProductName', 'ModelName']).agg(
        TotalQuantity=('OrderQuantity', 'sum'),
        TotalSales=('SalesAmount', 'sum'),
        AvgUnitPrice=('UnitPrice', 'mean'),
        OrderCount=('OrderQuantity', 'count')
    ).reset_index().sort_values('TotalSales', ascending=False)
    
    # Write to gold
    sales_summary_path = os.path.join(gold_dir, "sales_summary.parquet")
    sales_summary.to_parquet(sales_summary_path)
    
    return sales_summary


def create_customer_insights(silver_dir, gold_dir):
    """
    Create customer insights view in gold layer
    """
    # Read customers data from silver
    customers_path = os.path.join(silver_dir, "AdventureWorks_Customers.parquet")
    customers_df = pd.read_parquet(customers_path)
    
    # Read sales data from silver
    sales_path = os.path.join(silver_dir, "AdventureWorks_Sales.parquet")
    sales_df = pd.read_parquet(sales_path)
    
    # Join customers and sales
    joined_df = sales_df.merge(customers_df, on='CustomerKey')
    
    # Create customer insights
    customer_insights = joined_df.groupby(['CustomerKey', 'FirstName', 'LastName', 'Gender', 'MaritalStatus']).agg(
        TotalSpend=('SalesAmount', 'sum'),
        OrderCount=('OrderQuantity', 'count'),
        AvgOrderValue=('SalesAmount', 'mean')
    ).reset_index().sort_values('TotalSpend', ascending=False)
    
    # Write to gold
    customer_insights_path = os.path.join(gold_dir, "customer_insights.parquet")
    customer_insights.to_parquet(customer_insights_path)
    
    return customer_insights


def create_time_series_analysis(silver_dir, gold_dir):
    """
    Create time series analysis view in gold layer
    """
    # Read sales data from silver
    sales_path = os.path.join(silver_dir, "AdventureWorks_Sales.parquet")
    sales_df = pd.read_parquet(sales_path)
    
    # Read calendar data from silver
    calendar_path = os.path.join(silver_dir, "AdventureWorks_Calendar.parquet")
    calendar_df = pd.read_parquet(calendar_path)
    
    # Join sales and calendar
    joined_df = sales_df.merge(
        calendar_df,
        left_on='OrderDate',
        right_on='Date'
    )
    
    # Create time series analysis by month
    monthly_sales = joined_df.groupby(['Year', 'Month']).agg(
        MonthlySales=('SalesAmount', 'sum'),
        OrderCount=('OrderQuantity', 'count'),
        AvgOrderValue=('SalesAmount', 'mean')
    ).reset_index().sort_values(['Year', 'Month'])
    
    # Write to gold
    monthly_sales_path = os.path.join(gold_dir, "monthly_sales.parquet")
    monthly_sales.to_parquet(monthly_sales_path)
    
    return monthly_sales


def main():
    """
    Main function to process all silver data to gold
    """
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_dir = os.path.join(base_dir, "silver")
    gold_dir = os.path.join(base_dir, "gold")
    
    # Create gold directory if it doesn't exist
    os.makedirs(gold_dir, exist_ok=True)
    
    # Create sales summary
    create_sales_summary(silver_dir, gold_dir)
    print("Sales summary created successfully")
    
    # Create customer insights
    create_customer_insights(silver_dir, gold_dir)
    print("Customer insights created successfully")
    
    # Create time series analysis
    create_time_series_analysis(silver_dir, gold_dir)
    print("Time series analysis created successfully")


if __name__ == "__main__":
    main()