# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

import os
import sys
import pandas as pd 
import numpy as np
import warnings
import argparse

sys.path.append("../..")

from datetime import datetime
from tqdm import tqdm

from kpi_calculator.printing.KPI_printer import KPIExcelWriter
from kpi_calculator.log_parser.ocpp_2_0_1 import transaction_parser
from kpi_calculator.utils import fraction, time_ops

KPI_CALC_REPO_PATH = 'insert/path/to/repo/here'

PARSED_INPUT_FILE_NAME = 'parsed_messages_2025_05_19.csv'

START_RANGE = '2024-05-01'
END_RANGE = '2024-05-30'

LOGGING = True
    
def create_windowed_df(df: pd.DataFrame, window_start: str, window_end: str) -> pd.DataFrame: 
    windowed_df = df  
    if window_start != '':
        windowed_df = df[~(df['timestamp'].str.contains(window_start))]
    if window_end != '': 
        windowed_df = windowed_df[~(windowed_df['timestamp'].str.contains(window_end))]
    return windowed_df

def create_overlapped_window(df: pd.DataFrame, attribute_name: str, window_start: str, window_end: str, 
                             exclude_overlapping_values: list) -> pd.DataFrame:
    windowed_df = create_windowed_df(df, window_start, window_end)
    unique_overlapping_values = windowed_df[attribute_name].unique().tolist()
    for value in exclude_overlapping_values:
        if value not in unique_overlapping_values:
            continue 
        unique_overlapping_values.remove(value)
    overlapped_window = df[df[attribute_name].isin(unique_overlapping_values)]
    return overlapped_window
    
class InterimKPIs: 
    
    def __init__(self): 
        self.equations = {  1 : fraction.AdditiveFraction(),
                            3 : fraction.AdditiveFraction(),
                            4 : fraction.AdditiveFraction(),
                            5 : fraction.AdditiveFraction(),
                            9 : [],
                            10 : fraction.AdditiveFraction(),
                            12 : fraction.AdditiveFraction(),
                            14 : fraction.AdditiveFraction(),
                            15 : fraction.AdditiveFraction(),
                            16 : fraction.AdditiveFraction(),
        }
        
    def percentage_based_equation_registry(self, KPI_name: str) -> list[int]: 
        if KPI_name == 'session_success': 
            return [12, 14, 15, 16]
        elif KPI_name == 'charge_start_success':
            return [1, 3, 4, 5]
        elif KPI_name == 'charge_end_success': 
            return [10]
        else: 
            raise ValueError(f"KPI: {KPI_name} not a valid percentage-based KPI.")        
        
    def add_authorizes(self, num_authorizes: int) -> None: 
        self.equations[3].add_to_denominator(num_authorizes)
        self.equations[14].add_to_denominator(num_authorizes)
        
    def add_request_starts(self, num_request_starts: int) -> None: 
        self.equations[4].add_to_denominator(num_request_starts)
        self.equations[15].add_to_denominator(num_request_starts)
    
    def add_start(self, start: bool, mode: str) -> None:
        if  start is False: 
            return
        if mode == 'post_plugin': 
            self.equations[1].add_to_denominator(1)
            self.equations[12].add_to_denominator(1)
        elif mode == 'cached_auth': 
            self.equations[5].add_to_denominator(1)
            self.equations[16].add_to_denominator(1)
    
    def add_power_delivery_attempt(self, power_delivery_attempt: bool, mode: str) -> None: 
        if power_delivery_attempt is False: 
            return
        if mode == 'post_plugin':
            self.equations[1].add_to_numerator(1)
        elif mode == 'pre_plugin':
            self.equations[3].add_to_numerator(1)
        elif mode == 'request_start':
            self.equations[4].add_to_numerator(1)
        elif mode == 'cached_auth': 
            self.equations[5].add_to_numerator(1)
        self.equations[10].add_to_denominator(1)
        
    def add_valid_stop(self, valid_stop: bool, mode: str, power_delivery_attempt: bool) -> None: 
        if valid_stop is False: 
            return
        if mode == 'post_plugin':
            self.equations[12].add_to_numerator(1)
        elif mode == 'pre_plugin':
            self.equations[14].add_to_numerator(1)
        elif mode == 'request_start':
            self.equations[15].add_to_numerator(1)
        elif mode == 'cached_auth': 
            self.equations[16].add_to_numerator(1)
        if power_delivery_attempt is True:
            self.equations[10].add_to_numerator(1)
            
    def add_charge_start_time(self, transaction_df: pd.DataFrame) -> None: 
        start_timestamp, end_timestamp = transaction_parser.before_auth_timestamps(transaction_df)
        if start_timestamp is None: 
            start_timestamp, end_timestamp = transaction_parser.after_auth_timestamps(transaction_df)
        if start_timestamp is None: 
            return
        time_diff_seconds = time_ops.events_time_diff_seconds(start_timestamp, end_timestamp)
        self.equations[9].append(time_diff_seconds)
        
    def equation(self, equation_num: int) -> fraction.AdditiveFraction: 
        return self.equations[equation_num]
    
    def KPI_value(self, equation_num: int)-> float: 
        calculated_value = self.equations[equation_num].calculate_fraction()
        if calculated_value == 'undefined':
            return 'N/A'
        return calculated_value
        
    def total_numerator_for_x_KPI(self, KPI_name: str) -> int: 
        total_numerator = 0
        for equation_num in self.percentage_based_equation_registry(KPI_name):
            total_numerator += self.equations[equation_num].numerator
        return total_numerator     
                   
    def total_denominator_for_x_KPI(self, KPI_name: str) -> int: 
        total_denominator = 0
        for equation_num in self.percentage_based_equation_registry(KPI_name):
            total_denominator += self.equations[equation_num].denominator
        return total_denominator 
    
    def x_percentile_charge_start_time(self, percentile: int) -> float: 
        if self.num_charge_start_time_samples() == 0: 
            return -1
        charge_start_times = np.asarray(self.equations[9])
        return np.percentile(charge_start_times, percentile)
    
    def num_charge_start_time_samples(self) -> int: 
        return len(self.equations[9])
        
    def percent_contribution_for_x_KPI(self, equation_num: int, KPI_name: str) -> float: 
        percent_contribution_numerator = self.equations[equation_num].numerator
        total_denominator = self.total_denominator_for_x_KPI(KPI_name)
        if total_denominator == 0: 
            return 0
        return percent_contribution_numerator / total_denominator
    
    def weighted_average_for_x_KPI(self, KPI_name: str) -> float | str: 
        weighted_sum = 0
        sum_of_weights = 0
        for equation in self.percentage_based_equation_registry(KPI_name): 
            percent_contribution = self.percent_contribution_for_x_KPI(equation, KPI_name)
            if percent_contribution == 0 or self.equations[equation].calculate_fraction() == 'undefined': 
                continue
            weighted_sum += self.equations[equation].calculate_fraction() * percent_contribution
            sum_of_weights += percent_contribution
        if sum_of_weights == 0: 
            return 'N/A'
        return weighted_sum / sum_of_weights

    def print_KPIs(self, output_xlsx_file: str) -> None: 
        xlsx_writer = KPIExcelWriter(output_xlsx_file)
        xlsx_writer.write_session_success(self)
                 
    
class KPICalculator: 

    def __init__(self, df: pd.DataFrame):
        self._windowed_df = create_windowed_df(df, START_RANGE, END_RANGE)
        self._overlapped_windowed_df = create_overlapped_window(df, 'transaction_ID', START_RANGE, END_RANGE, [-1])
        self._interim_KPIs = InterimKPIs()
        
    def tabulate_orphan_authorizes(self):
        authorizes_df = transaction_parser.filter_authorizes(self._windowed_df)
        orphaned_authorizes =  authorizes_df[authorizes_df['transaction_ID'] == -1]
        orphaned_authorizes_1 = authorizes_df[authorizes_df['transaction_ID'] == -98]
        orphaned_authorizes_2 =  authorizes_df[authorizes_df['transaction_ID'] == -99]
        self._interim_KPIs.add_authorizes(len(orphaned_authorizes) + len(orphaned_authorizes_1) + len(orphaned_authorizes_2))
    
    def tabulate_orphan_request_starts(self) -> None:
        request_starts_df = transaction_parser.filter_request_starts(self._windowed_df)
        orphaned_request_starts = len(request_starts_df[pd.isna(request_starts_df['transaction_ID'])])
        self._interim_KPIs.add_request_starts(orphaned_request_starts)

    def tabulate_transactional_values(self) -> None: 
        unique_transaction_IDs = self._overlapped_windowed_df['transaction_ID'].unique().tolist()
        print('-------Tabulating Transaction Values------')
        for transaction_ID in tqdm(unique_transaction_IDs):
            transaction_df = self._overlapped_windowed_df[self._overlapped_windowed_df['transaction_ID'] == transaction_ID]
            transaction_authorizes = len(transaction_parser.filter_authorizes_no_double_count(transaction_df))
            transaction_request_starts = len(transaction_parser.filter_request_starts(transaction_df))
            power_delivery_attempt = transaction_parser.power_delivery_attempt(transaction_df)
            valid_start = transaction_parser.valid_start(transaction_df)
            valid_stop = transaction_parser.valid_stop(transaction_df)
            if transaction_parser.valid_auth_start(transaction_df):
                self._interim_KPIs.add_start(True, 'cached_auth')
                self._interim_KPIs.add_power_delivery_attempt(power_delivery_attempt, 'cached_auth')
                self._interim_KPIs.add_valid_stop(valid_stop, 'cached_auth', power_delivery_attempt)
            elif transaction_request_starts != 0: 
                self._interim_KPIs.add_authorizes(transaction_authorizes)
                self._interim_KPIs.add_request_starts(transaction_request_starts)
                self._interim_KPIs.add_power_delivery_attempt(power_delivery_attempt, 'request_start')
                self._interim_KPIs.add_valid_stop(valid_stop, 'request_start', power_delivery_attempt)
            elif transaction_authorizes != 0: 
                self._interim_KPIs.add_authorizes(transaction_authorizes)
                self._interim_KPIs.add_power_delivery_attempt(power_delivery_attempt, 'pre_plugin')
                self._interim_KPIs.add_valid_stop(valid_stop, 'pre_plugin', power_delivery_attempt)
            elif valid_start: 
                self._interim_KPIs.add_start(valid_start, 'post_plugin')
                self._interim_KPIs.add_power_delivery_attempt(power_delivery_attempt, 'post_plugin')
                self._interim_KPIs.add_valid_stop(valid_stop, 'post_plugin', power_delivery_attempt) 
            self._interim_KPIs.add_charge_start_time(transaction_df)
            
    def print_KPIs(self, output_xlsx_file: str) -> None: 
        xlsx_writer = KPIExcelWriter(output_xlsx_file)
        succession_success_equations = self._interim_KPIs.percentage_based_equation_registry('session_success')
        xlsx_writer.write_percentage_based_KPI_sheet(self._interim_KPIs, 'completions', 'charge_attempts', 
                                                     'session_success', succession_success_equations)
        charge_start_success_equations = self._interim_KPIs.percentage_based_equation_registry('charge_start_success')
        xlsx_writer.write_percentage_based_KPI_sheet(self._interim_KPIs, 'power_delivery_attempts', 'plug_in_attempts', 
                                                     'charge_start_success', charge_start_success_equations)
        charge_end_success_equations = self._interim_KPIs.percentage_based_equation_registry('charge_end_success')
        xlsx_writer.write_percentage_based_KPI_sheet(self._interim_KPIs, 'completions', 'power_delivery_attempts', 
                                                     'charge_end_success', charge_end_success_equations, True)
        xlsx_writer.write_charge_start_time(self._interim_KPIs)
        xlsx_writer.write_KPIs()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='interim-kpi-calculator')
    parser.add_argument('--start_date', '-s', help='start date of the given dataset (this is used to truncate overlapping days)')
    parser.add_argument('--end_date', '-e', help='end date of the given dataset (this is used to truncate overlapping days)')
    parser.add_argument('--parsed_file', '-pf', help='parsed input file to analyze with the kpi calculator')
    args = parser.parse_args()

    if(args.parsed_file != None):
        PARSED_INPUT_FILE_NAME = args.parsed_file

    if(args.start_date != None):
        START_RANGE = args.start_date

    if(args.end_date != None):
        END_RANGE = args.end_date

    input_data_path = KPI_CALC_REPO_PATH + "/interim-kpi-calculator/data/parsed_logs/" + PARSED_INPUT_FILE_NAME
    output_data_dir = KPI_CALC_REPO_PATH + "/interim-kpi-calculator/data/KPIs"

    if not os.path.exists(output_data_dir):
        os.mkdir(output_data_dir)
    df = pd.read_csv(input_data_path)
    df = df.drop_duplicates(subset=['device_ID', 'transaction_ID', 'event_type', 'event_code', 'timestamp'], keep='first')
    if len(df.index) == 0:
        raise ValueError('Formatted data is empty. Cannot perform calculations')
    KPI_calculator = KPICalculator(df)
    KPI_calculator.tabulate_orphan_authorizes()
    KPI_calculator.tabulate_orphan_request_starts()
    KPI_calculator.tabulate_transactional_values()
    todays_date = datetime.today().strftime('%Y-%m-%d')
    output_file_path = os.path.join(output_data_dir, f"dataset_KPIs_{todays_date}.xlsx")
    KPI_calculator.print_KPIs(output_file_path)