#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main Script for Adventure Works Data Pipeline

This script orchestrates the entire data pipeline, allowing users to run specific layers
or all layers via command-line arguments.
"""

import os
import argparse
import sys
from bronze_to_silver import main as bronze_to_silver_main
from silver_to_gold import main as silver_to_gold_main
from visualize import main as visualize_main


def run_bronze_to_silver():
    """
    Run the bronze to silver layer transformation
    """
    print("\n===== Running Bronze to Silver Layer Transformation =====")
    bronze_to_silver_main()


def run_silver_to_gold():
    """
    Run the silver to gold layer transformation
    """
    print("\n===== Running Silver to Gold Layer Transformation =====")
    silver_to_gold_main()


def run_visualizations():
    """
    Run the visualization module
    """
    print("\n===== Creating Visualizations =====")
    visualize_main()


def main():
    """
    Main function to orchestrate the data pipeline
    """
    parser = argparse.ArgumentParser(description='Adventure Works Data Pipeline')
    parser.add_argument(
        '--layers', 
        choices=['bronze_to_silver', 'silver_to_gold', 'visualize', 'all'],
        default='all',
        help='Specify which layers to run'
    )
    
    args = parser.parse_args()
    
    print("\n===== Adventure Works Data Pipeline =====")
    print(f"Running layers: {args.layers}")
    
    # Run specified layers
    if args.layers == 'all' or args.layers == 'bronze_to_silver':
        run_bronze_to_silver()
    
    if args.layers == 'all' or args.layers == 'silver_to_gold':
        run_silver_to_gold()
    
    if args.layers == 'all' or args.layers == 'visualize':
        run_visualizations()
    
    print("\n===== Data Pipeline Completed Successfully =====")


if __name__ == "__main__":
    main()