# COMP810 Data Warehouse and Big Data

This repository contains the Hive queries and MapReduce pseudo-code used for the project **Big Data Analysis of Customer Churn Factors Using Large-Scale Telecommunications Data**.

## Project Summary

The project analyses customer churn patterns using a telecommunications dataset containing approximately 1,000,000 customer records.

The workflow uses:

- Amazon S3 for data storage
- Amazon EMR and Apache Hive for data validation, cleaning, statistical analysis, and aggregation
- Hue for visualisation
- MapReduce pseudo-code for distributed churn aggregation

## Files

- `01_data_validation_and_cleaning_emr_hue.hql` — data validation, cleaning, transformation, and descriptive statistics
- `customer_churn_final_visualisation_queries.hql` — Hive queries used for the final churn visualisations
- `mapreduce_churn_pseudocode.md` — MapReduce design for churn analysis by contract type and payment method

## Dataset

Customer Churn Prediction Dataset_1M  
https://www.kaggle.com/datasets/isandeep06/customer-churn-prediction-dataset-1m

## Course

COMP810 — Data Warehouse and Big Data  
Auckland University of Technology
