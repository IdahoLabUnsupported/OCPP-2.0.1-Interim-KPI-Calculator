# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

import pandas as pd

from kpi_calculator.log_parser.ocpp_2_0_1.status_event import code, type as event_type 
   
   
def valid_stop(df: pd.DataFrame) -> bool: 
    valid_stop_df = df[(df['trigger_reason'] == 'EnergyLimitReached') |
                        (df['trigger_reason'] == 'SOCLimitReached') | 
                        (df['trigger_reason'] == 'Local') |
                        (df['trigger_reason'] == 'Remote') | 
                        (df['trigger_reason'] == 'StoppedByEV') | 
                        (df['trigger_reason'] == 'LocalOutofCredit') |
                        (df['trigger_reason'] == 'TimeLimitReached') | 
                        (df['trigger_reason'] == 'EVDisconnected')]
    if not valid_stop_df.empty:
        return True
    return False

def get_valid_starts(df: pd.DataFrame) -> pd.DataFrame: 
    return df[(df['event_code'] == code.STARTED) & (df['trigger_reason'] == code.CABLE_PLUGGED_IN)]
    
def valid_start(df: pd.DataFrame) -> bool: 
    valid_start_df = get_valid_starts(df)
    if not valid_start_df.empty:
        return True
    return False
    
def valid_auth_start(df: pd.DataFrame) -> bool: 
    valid_auth_start = df[(df['event_code'] == code.STARTED) & 
                            ((df['trigger_reason'] == code.ACCEPTED) | 
                            (df['trigger_reason'] == code.REJECTED))]
    if not valid_auth_start.empty:
        return True
    return False   

def get_power_delivery_attempts(df: pd.DataFrame) -> bool: 
    power_delivery_attempt = df[((df['event_code'] == code.CHARGING) & 
                        (df['trigger_reason'] == code.CHARGING_STATE_CHANGED))]   
    return power_delivery_attempt

def power_delivery_attempt(df: pd.DataFrame) -> bool: 
    power_delivery_attempts = get_power_delivery_attempts(df)
    if not power_delivery_attempts.empty: 
        return True
    return False

def before_auth_timestamps(df: pd.DataFrame) -> tuple[str, str] | tuple[None, None]: 
    power_delivery_attempts = get_power_delivery_attempts(df)
    # finding a non-empty response will encompass all authorize types
    response_timestamps = df[~pd.isna(df['response_timestamp'])]
    if power_delivery_attempts.empty or response_timestamps.empty: 
        return None, None
    return response_timestamps.iloc[0]['response_timestamp'], power_delivery_attempts.iloc[0]['timestamp']
  
def after_auth_timestamps(df: pd.DataFrame) -> tuple[str, str] | tuple[None, None]:
    power_delivery_attempts = get_power_delivery_attempts(df)
    valid_starts = get_valid_starts(df)
    if power_delivery_attempts.empty or valid_starts.empty: 
        return None, None
    return valid_starts.iloc[0]['timestamp'], power_delivery_attempts.iloc[0]['timestamp']

def filter_request_starts(df: pd.DataFrame) -> pd.DataFrame: 
    request_starts_df = df[(df['event_type'] == \
                                    event_type.REQUEST_START_TRANSACTION_RESPONSE) & 
                            ((df['event_code'] == code.ACCEPTED) | 
                            (df['event_code'] == code.REJECTED))]
    return request_starts_df
    
def filter_authorizes(df: pd.DataFrame) -> pd.DataFrame: 
    authorizes_df = df[(df['event_type'] == event_type.AUTHORIZE_RESPONSE) | 
                        ((df['event_code'] == code.STARTED) & 
                        ((df['trigger_reason'] == code.ACCEPTED) | (df['trigger_reason'] == code.REJECTED)))]
    return authorizes_df
    
def filter_authorizes_no_double_count(df: pd.DataFrame) -> pd.DataFrame: 
    authorizes_df = df[(df['event_type'] == event_type.AUTHORIZE_RESPONSE) | 
                        ((df['event_code'] == code.STARTED) & 
                        ((df['trigger_reason'] == code.ACCEPTED) | (df['trigger_reason'] == code.REJECTED)))]
    unique_event_types = df['event_type'].unique().tolist()
    unique_trigger_reasons = df['trigger_reason'].unique().tolist()
    if event_type.AUTHORIZE_RESPONSE in unique_event_types and \
        code.ACCEPTED in unique_trigger_reasons:
        authorizes_df = authorizes_df[authorizes_df['event_type'] != event_type.AUTHORIZE_RESPONSE]
    return authorizes_df