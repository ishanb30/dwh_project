# dwh_project
A learning project exploring the basic structure of a data warehouse

## Context
This project is being built alongside a tutorial to understand core data warehousing concepts. 
It focuses on how raw data is organised into distinct layers, how transformations are applied 
between those layers, and how analytical queries are supported by the final schema.

The tutorial introduces common warehouse patterns such as staging and analytical layers, 
basic dimensional modelling, and SQL-based transformations that move data from raw inputs 
to queryable tables.

## Data source assumptions
This project assumes the presence of raw CSV files stored outside the repository.
Raw datasets are not version-controlled to reflect real-world data engineering
practices. The bronze layer table definitions act as the contract for the expected
structure of source data.
