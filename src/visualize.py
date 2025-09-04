#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Visualization Module

This script generates visualizations from the gold layer data using pandas and matplotlib.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def create_monthly_sales_trend(gold_dir, output_dir):
    """
    Create monthly sales trend visualization
    """
    # Read monthly sales data from gold
    monthly_sales_path = os.path.join(gold_dir, "monthly_sales.parquet")
    monthly_sales_df = pd.read_parquet(monthly_sales_path)
    
    # Create date column for better plotting
    monthly_sales_df['YearMonth'] = monthly_sales_df['Year'].astype(str) + '-' + monthly_sales_df['Month'].astype(str).str.zfill(2)
    
    # Plot monthly sales trend
    plt.figure(figsize=(12, 6))
    plt.plot(monthly_sales_df['YearMonth'], monthly_sales_df['MonthlySales'], marker='o', linewidth=2)
    plt.title('Monthly Sales Trend', fontsize=16)
    plt.xlabel('Year-Month', fontsize=12)
    plt.ylabel('Sales Amount ($)', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save the plot
    output_path = os.path.join(output_dir, "monthly_sales_trend.png")
    plt.savefig(output_path)
    plt.close()
    
    print(f"Monthly sales trend visualization saved to {output_path}")


def create_top_products_chart(gold_dir, output_dir):
    """
    Create top products chart visualization
    """
    # Read sales summary data from gold
    sales_summary_path = os.path.join(gold_dir, "sales_summary.parquet")
    sales_summary_df = pd.read_parquet(sales_summary_path)
    
    # Get top 10 products by sales
    top_products = sales_summary_df.sort_values('TotalSales', ascending=False).head(10)
    
    # Plot top products
    plt.figure(figsize=(12, 8))
    bars = plt.barh(top_products['ProductName'], top_products['TotalSales'])
    plt.title('Top 10 Products by Sales', fontsize=16)
    plt.xlabel('Total Sales ($)', fontsize=12)
    plt.ylabel('Product Name', fontsize=12)
    plt.grid(True, axis='x', alpha=0.3)
    
    # Add value labels to bars
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 1000, bar.get_y() + bar.get_height()/2, f'${width:,.0f}', 
                ha='left', va='center', fontsize=10)
    
    plt.tight_layout()
    
    # Save the plot
    output_path = os.path.join(output_dir, "top_products.png")
    plt.savefig(output_path)
    plt.close()
    
    print(f"Top products visualization saved to {output_path}")


def create_customer_segments(gold_dir, output_dir):
    """
    Create customer segments visualization
    """
    # Read customer insights data from gold
    customer_insights_path = os.path.join(gold_dir, "customer_insights.parquet")
    customer_insights_df = pd.read_parquet(customer_insights_path)
    
    # Create spending segments
    customer_insights_df['SpendingSegment'] = pd.qcut(
        customer_insights_df['TotalSpend'], 
        q=4, 
        labels=['Low', 'Medium', 'High', 'Premium']
    )
    
    # Aggregate by segment
    segment_summary = customer_insights_df.groupby('SpendingSegment').agg(
        CustomerCount=('CustomerKey', 'count'),
        TotalRevenue=('TotalSpend', 'sum'),
        AvgSpend=('TotalSpend', 'mean')
    ).reset_index()
    
    # Plot customer segments
    plt.figure(figsize=(10, 6))
    
    # Create pie chart
    plt.subplot(1, 2, 1)
    plt.pie(segment_summary['CustomerCount'], labels=segment_summary['SpendingSegment'], 
            autopct='%1.1f%%', startangle=90, shadow=True)
    plt.title('Customer Distribution by Spending Segment', fontsize=14)
    
    # Create bar chart
    plt.subplot(1, 2, 2)
    sns.barplot(x='SpendingSegment', y='AvgSpend', data=segment_summary)
    plt.title('Average Spend by Segment', fontsize=14)
    plt.ylabel('Average Spend ($)', fontsize=12)
    plt.grid(True, axis='y', alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    output_path = os.path.join(output_dir, "customer_segments.png")
    plt.savefig(output_path)
    plt.close()
    
    print(f"Customer segments visualization saved to {output_path}")


def main():
    """
    Main function to generate all visualizations
    """
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gold_dir = os.path.join(base_dir, "gold")
    output_dir = os.path.join(base_dir, "visualizations")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create visualizations
    create_monthly_sales_trend(gold_dir, output_dir)
    create_top_products_chart(gold_dir, output_dir)
    create_customer_segments(gold_dir, output_dir)
    
    print("All visualizations created successfully")


if __name__ == "__main__":
    main()