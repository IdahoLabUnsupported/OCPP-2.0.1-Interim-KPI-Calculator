# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

import os
import pandas as pd

KPI_CALC_REPO_PATH = 'insert/path/to/repo/here'

if __name__ == "__main__": 
    raw_log_dir = KPI_CALC_REPO_PATH + "/interim-kpi-calculator/data/cleaned_logs"
    output_dir = KPI_CALC_REPO_PATH + "/interim-kpi-calculator/data/split_logs"
    if not os.path.exists(output_dir): 
        os.mkdir(output_dir)
    dfs = []
    for raw_log in os.listdir(raw_log_dir):
        df = pd.read_csv(os.path.join(raw_log_dir, raw_log))
        dfs.append(df)
    new_df = pd.concat(dfs)
    unique_device_IDs = new_df['device_ID'].unique().tolist()
    for device_ID in unique_device_IDs: 
        single_device_logs = new_df[new_df['device_ID'] == device_ID]
        single_device_logs.to_csv(os.path.join(output_dir, str(device_ID) + '.csv'), index=False)