# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

import typing 
import pandas as pd


def hourly_blocks(df: pd.DataFrame, column_name: str) -> typing.Generator[str, pd.DataFrame]:
    for hour in range(1,24):
        hourly_block = df[df[column_name].dt.hour == hour]
        yield hour, hourly_block

def sort(df: pd.DataFrame, sort_by_column_name: str) -> pd.DataFrame:
    if sort_by_column_name not in df.columns:
        raise ValueError(f"{sort_by_column_name} not a recognized column name in logs")
    sorted_df = df.sort_values(by=sort_by_column_name)
    return sorted_df

def remove_rows_with_values(df: pd.DataFrame, values: list[typing.Any], values_column_name: str) -> pd.DataFrame: 
    removed_rows_df = df
    for value in values: 
        removed_rows_df = removed_rows_df[removed_rows_df[values_column_name] == value]
    return removed_rows_df
