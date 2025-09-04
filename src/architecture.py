#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Architecture Diagram Generator

This script generates a simple architecture diagram for the Adventure Works data pipeline.
"""

import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np


def create_architecture_diagram(output_path):
    """
    Create and save an architecture diagram for the data pipeline
    """
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Define colors
    bronze_color = '#CD7F32'
    silver_color = '#C0C0C0'
    gold_color = '#FFD700'
    arrow_color = '#555555'
    process_color = '#4472C4'
    
    # Define layer positions
    bronze_y = 0.8
    silver_y = 0.5
    gold_y = 0.2
    
    # Draw Bronze Layer
    bronze_rect = patches.Rectangle((0.1, bronze_y-0.1), 0.2, 0.2, linewidth=1, edgecolor='black', facecolor=bronze_color, alpha=0.7)
    ax.add_patch(bronze_rect)
    ax.text(0.2, bronze_y, 'Bronze Layer', ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Draw Bronze Data Sources
    data_sources = ['Calendar', 'Customers', 'Products', 'Sales', 'Returns', 'Territories']
    for i, source in enumerate(data_sources):
        y_pos = bronze_y - 0.15 - (i * 0.05)
        ax.text(0.35, y_pos, f'CSV: {source}', ha='left', va='center', fontsize=10)
    
    # Draw Silver Layer
    silver_rect = patches.Rectangle((0.4, silver_y-0.1), 0.2, 0.2, linewidth=1, edgecolor='black', facecolor=silver_color, alpha=0.7)
    ax.add_patch(silver_rect)
    ax.text(0.5, silver_y, 'Silver Layer', ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Draw Silver Data Sources
    silver_sources = ['Cleaned Calendar', 'Cleaned Customers', 'Cleaned Products', 'Combined Sales', 'Cleaned Returns']
    for i, source in enumerate(silver_sources):
        y_pos = silver_y - 0.15 - (i * 0.05)
        ax.text(0.65, y_pos, f'Parquet: {source}', ha='left', va='center', fontsize=10)
    
    # Draw Gold Layer
    gold_rect = patches.Rectangle((0.7, gold_y-0.1), 0.2, 0.2, linewidth=1, edgecolor='black', facecolor=gold_color, alpha=0.7)
    ax.add_patch(gold_rect)
    ax.text(0.8, gold_y, 'Gold Layer', ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Draw Gold Views
    gold_views = ['Sales Summary', 'Customer Insights', 'Monthly Sales', 'Product Analytics']
    for i, view in enumerate(gold_views):
        y_pos = gold_y - 0.15 - (i * 0.05)
        ax.text(0.95, y_pos, f'View: {view}', ha='left', va='center', fontsize=10)
    
    # Draw Process Arrows and Boxes
    # Bronze to Silver
    arrow = patches.FancyArrowPatch((0.3, bronze_y), (0.4, silver_y), 
                                  connectionstyle="arc3,rad=.1", 
                                  arrowstyle="->", 
                                  mutation_scale=20, 
                                  linewidth=2, 
                                  color=arrow_color)
    ax.add_patch(arrow)
    process_rect = patches.Rectangle((0.32, (bronze_y+silver_y)/2-0.05), 0.06, 0.1, 
                                   linewidth=1, edgecolor='black', facecolor=process_color, alpha=0.7)
    ax.add_patch(process_rect)
    ax.text(0.35, (bronze_y+silver_y)/2, 'ETL', ha='center', va='center', fontsize=10, color='white')
    
    # Silver to Gold
    arrow = patches.FancyArrowPatch((0.6, silver_y), (0.7, gold_y), 
                                  connectionstyle="arc3,rad=.1", 
                                  arrowstyle="->", 
                                  mutation_scale=20, 
                                  linewidth=2, 
                                  color=arrow_color)
    ax.add_patch(arrow)
    process_rect = patches.Rectangle((0.62, (silver_y+gold_y)/2-0.05), 0.06, 0.1, 
                                   linewidth=1, edgecolor='black', facecolor=process_color, alpha=0.7)
    ax.add_patch(process_rect)
    ax.text(0.65, (silver_y+gold_y)/2, 'Agg', ha='center', va='center', fontsize=10, color='white')
    
    # Add title
    ax.text(0.5, 0.95, 'Adventure Works Data Engineering Pipeline', ha='center', va='center', fontsize=16, fontweight='bold')
    
    # Remove axes
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    # Save diagram
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()


def main():
    """
    Main function to generate architecture diagram
    """
    # Define paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(base_dir, "docs")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create and save architecture diagram
    output_path = os.path.join(output_dir, "architecture_diagram.png")
    create_architecture_diagram(output_path)
    print(f"Architecture diagram saved to {output_path}")


if __name__ == "__main__":
    main()