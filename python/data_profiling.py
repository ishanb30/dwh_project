"""
Source Data Profiling

Analyzes all CSV files in source directory to calculate data quality metrics
(length bounds, null rates, cardinality, type inference) that inform
Bronze table design and Silver transformation requirements.

Outputs: data_profiling_results.csv with one row per column across all files.
"""

import pandas as pd
from pandas.api.types import is_numeric_dtype
from config import BASE_DIR
from config import SOURCE_CSV_DIR

files = list(SOURCE_CSV_DIR.rglob("*.csv"))

def length_and_numeric_checks(series, temp_numeric):
    max_len = series.dropna().astype(str).str.len().max()
    min_len = series.dropna().astype(str).str.len().min()
    num_max = None
    num_min = None
    if is_numeric_dtype(series):
        num_max = series.max()
        num_min = series.min()
    elif temp_numeric.notnull().any():
        num_max = temp_numeric.max()
        num_min = temp_numeric.min()

    return {
        "Max Length": max_len,
        "Min Length": min_len,
        "Numeric Max": num_max,
        "Numeric Min": num_min,
    }

def basic_column_metrics(series):
    null_pct = series.isnull().mean()
    unique_cnt = series.nunique()
    row_cnt = series.shape[0]
    top3_common_values_str = "High Cardinality"
    if unique_cnt < 0.15 * row_cnt:
        top3_common_values_series = series.value_counts().head(3)
        top3_common_values_str = ", ".join([f"{value}({count})" for value, count in top3_common_values_series.items()])

    return {
        "Null Pct": null_pct,
        "Cardinality": unique_cnt,
        "Row Count": row_cnt,
        "Top 3 Common Values": top3_common_values_str
    }


def data_type(series, temp_numeric):
    boolean = [True, False,1,0]
    if series.isnull().all():
        return "Empty"
    if set(series.dropna().unique()).issubset(boolean):
        return "Boolean"
    if is_numeric_dtype(series):
        return "Numeric"
    if temp_numeric.notnull().any():
        return "Numeric as String"

    converted_date = pd.to_datetime(series, errors="coerce")
    if converted_date.notnull().any():
        return "Date as String"

    return "String"


profile_results = []

for file in files:
    name = file.stem
    source_df = pd.read_csv(file)

    for column in source_df:
        series = source_df[column]
        temp_numeric = pd.to_numeric(series, errors="coerce")


        len_and_num = length_and_numeric_checks(series, temp_numeric)
        col_metrics = basic_column_metrics(series)
        dtype = data_type(series, temp_numeric)

        row_dict = {
            "Name": name,
            "Column": column,
            "Max Length": len_and_num["Max Length"],
            "Min Length": len_and_num["Min Length"],
            "Numeric Max": len_and_num["Numeric Max"],
            "Numeric Min": len_and_num["Numeric Min"],
            "Null Pct": col_metrics["Null Pct"],
            "Cardinality": col_metrics["Cardinality"],
            "Row Count": col_metrics["Row Count"],
            "Top 3 Common Values": col_metrics["Top 3 Common Values"],
            "Data Type": dtype
        }

        profile_results.append(row_dict)

results_df = pd.DataFrame(profile_results)
results_df.to_csv(BASE_DIR / "python" / "data_profiling_results.csv", index=False)


