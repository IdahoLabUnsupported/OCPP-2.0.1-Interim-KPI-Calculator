# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

import os
import sys
import pandas as pd 
import json
import warnings

sys.path.append("..")

from datetime import datetime, timedelta
from tqdm import tqdm

from kpi_calculator.log_parser.ocpp_2_0_1 import message as message_structure
from kpi_calculator.log_parser.ocpp_2_0_1.status_event import type as event_type, code
from kpi_calculator.utils import time_ops

KPI_CALC_REPO_PATH = 'insert/path/to/repo/here'

AUTHORIZE_TIME_THRESHOLD_SECONDS = timedelta(minutes=5).total_seconds()

GENERAL_EVENT_TYPES = [event_type.AUTHORIZE_RESPONSE, event_type.REQUEST_START_TRANSACTION_RESPONSE, 
                        event_type.STATUS_NOTIFICATION_REQUEST, event_type.TRANSACTION_EVENT_REQUEST]

def has_relevant_event(message: list) -> bool:
    #print(message[0])
    if type(message) is not list:
        return False
    if message[message_structure.EVENT_TYPE_INDEX] == event_type.METER_VALUES:
        if 'chargingState' not in message[message_structure.INTERIOR_MESSAGE_INDEX].keys():
            return False
        if message[message_structure.INTERIOR_MESSAGE_INDEX]['chargingState'] == code.CHARGING:
            return True
    #print("YES")
    if message[message_structure.EVENT_TYPE_INDEX] in GENERAL_EVENT_TYPES: 
        #print("YES")
        return True
    return False
    
def message_formatted_as_response(row: pd.Series) -> bool:
    if type(row['message']) is not list:
        return False   
    if len(row['message']) < message_structure.RESPONSE_INTERIOR_MESSAGE_INDEX + 1:
        return False
    message = row['message'][message_structure.RESPONSE_INTERIOR_MESSAGE_INDEX]
    if type(message) is not dict: 
        return False 
    return True 
    
def is_valid_non_request_response(message: dict) -> bool: 
    if 'idTokenInfo' in message.keys():
        return True
    return False  

def is_request_start_response_accepted(message: dict) -> bool: 
    # Pretty large assumption here. TransactionId did not appear in RequestStopResponses
    # Assuming that combination  status/transactionId should only be an attribute value in RequestStartResponses    
    if 'status' in message.keys() and 'transactionId' in message.keys():
        return True
    return False

def is_request_start_response_rejected(message: dict) -> bool:
    if 'statusInfo' not in message.keys(): 
        return False
    if 'reasonCode' not in message['statusInfo'].keys():
        return False
    if message['statusInfo']['reasonCode'] == "SessionStartRejected": 
        return True
    return False
       
def get_response(message_ID: int, timestamp: str, original_df: pd.DataFrame) -> tuple[str, str, str]:
    df_with_message_ID = original_df[original_df['message_ID'] == message_ID]
    df_with_message_ID = df_with_message_ID.sort_values(by=['timestamp'])
    # removing all records with timestamps before the message will make it so
    # the transaction IDs for Request Starts pair correctly (i.e. chronologically)
    no_events_before_timestamp_df = df_with_message_ID[df_with_message_ID['timestamp'] >= timestamp]
    for _, row in no_events_before_timestamp_df.iterrows(): 
        if not message_formatted_as_response(row):
            continue
        message = row['message'][message_structure.RESPONSE_INTERIOR_MESSAGE_INDEX]
        if is_valid_non_request_response(message):
            return message['idTokenInfo']['status'], pd.NA, row['timestamp']
        if is_request_start_response_accepted(message):
            return message['status'], message['transactionId'], row['timestamp']
        if is_request_start_response_rejected(message):
            return message['status'], pd.NA, row['timestamp']
    return 'Unknown', pd.NA, pd.NA
        
def get_message_info(message: dict, original_df: pd.DataFrame, 
                                      message_ID: int, timestamp: str,) -> tuple[str, str, str]: 
    if message[message_structure.EVENT_TYPE_INDEX] == event_type.AUTHORIZE_RESPONSE or \
       message[message_structure.EVENT_TYPE_INDEX] == event_type.REQUEST_START_TRANSACTION_RESPONSE: 
        return get_response(message_ID, timestamp, original_df)
    if 'transactionInfo' in message[message_structure.INTERIOR_MESSAGE_INDEX].keys():
        if 'stoppedReason' in message[message_structure.INTERIOR_MESSAGE_INDEX]['transactionInfo'].keys(): 
            return 'Ended', message[message_structure.INTERIOR_MESSAGE_INDEX]['transactionInfo']['transactionId'], pd.NA
        if message[message_structure.INTERIOR_MESSAGE_INDEX]['eventType'] == 'Started': 
            return 'Started', message[message_structure.INTERIOR_MESSAGE_INDEX]['transactionInfo']['transactionId'], pd.NA      
    if message[message_structure.EVENT_TYPE_INDEX] == event_type.STATUS_NOTIFICATION_REQUEST:
        return message[message_structure.INTERIOR_MESSAGE_INDEX]['connectorStatus'], pd.NA, pd.NA
    if message[message_structure.INTERIOR_MESSAGE_INDEX]['eventType'] == 'Updated' and \
        'transactionInfo' in message[message_structure.INTERIOR_MESSAGE_INDEX].keys(): 
        if 'chargingState' not in message[message_structure.INTERIOR_MESSAGE_INDEX]['transactionInfo']:
            return 'remove', pd.NA, pd.NA
        return message[message_structure.INTERIOR_MESSAGE_INDEX]['transactionInfo']['chargingState'], \
               message[message_structure.INTERIOR_MESSAGE_INDEX]['transactionInfo']['transactionId'], \
               pd.NA
    return message[message_structure.INTERIOR_MESSAGE_INDEX]['eventType'], pd.NA, pd.NA

def get_event_type(message: dict) -> str:
    return message[message_structure.EVENT_TYPE_INDEX]

def get_general_attribute(message: dict, attribute: str) -> str: 
    if attribute in message[message_structure.INTERIOR_MESSAGE_INDEX].keys(): 
        return message[message_structure.INTERIOR_MESSAGE_INDEX][attribute]
    return pd.NA

def get_authorized_start_message_info(message:dict, message_ID: str, timestamp: str, event_code: str, original_df: pd.DataFrame) -> str: 
    if event_code == 'Ended': 
        return message[message_structure.INTERIOR_MESSAGE_INDEX]['transactionInfo']['stoppedReason'], pd.NA
    trigger_reason = get_general_attribute(message, 'triggerReason')
    response_timestamp = pd.NA
    if event_code == 'Started' and trigger_reason == 'Authorized':
        trigger_reason, _, response_timestamp = get_response(message_ID, timestamp, original_df)
        if trigger_reason == 'Unknown':
            trigger_reason = 'Rejected'
    return trigger_reason, response_timestamp

def get_message_ID(message: list) -> str: 
    if type(message) is not list: 
        return pd.NA
    else: 
        return message[message_structure.MESSAGE_ID_INDEX]

def get_ID_token(message: list) -> str: 
    if type(message) is not list: 
        return pd.NA 
    if len(message) < message_structure.INTERIOR_MESSAGE_INDEX + 1:
        return pd.NA
    interior_message = message[message_structure.INTERIOR_MESSAGE_INDEX]
    if 'idToken'not in interior_message.keys(): 
        return pd.NA
    if 'idToken' not in interior_message['idToken'].keys():
        return pd.NA
    return interior_message['idToken']['idToken']

def add_status_event(df_attributes: tuple, row: pd.Series, original_df: pd.DataFrame) -> tuple[list, list]:
    device_IDs, ID_tokens, transaction_IDs, event_types, event_codes, trigger_reasons, \
        timestamps, response_timestamps = df_attributes
    message = row['message']
    message_ID = row['message_ID']
    device_IDs.append(row['device_ID'])
    ID_tokens.append(row['ID_token'])
    event_types.append(get_event_type(message))
    event_code, transaction_ID, response_timestamp = get_message_info(message, original_df,
                                                                      message_ID, row['timestamp'])
    # always add a response timestamp even if it's pd.NA
    response_timestamps.append(response_timestamp)
    event_codes.append(event_code)
    transaction_IDs.append(transaction_ID)
    trigger_reason, response_timestamp = get_authorized_start_message_info(message, message_ID, row['timestamp'], \
                                                                            event_code, original_df)
    # modify final value if the second response timestamp isn't pd.NA
    if not pd.isna(response_timestamp) and pd.isna(response_timestamps[-1]): 
        response_timestamps[-1] = response_timestamp   
    trigger_reasons.append(trigger_reason)
    timestamps.append(row['timestamp'])
    return device_IDs, ID_tokens, transaction_IDs, event_types, \
            event_codes, trigger_reasons, timestamps, response_timestamps

def surrounding_events_have_same_ID(df: pd.DataFrame) -> bool: 
    if df.iloc[0]['connector_ID'] == df.iloc[2]['connector_ID']:
        return True
    return False

def surrounding_events_in_time_threshold(df: pd.DataFrame) -> bool: 
    time_diff_1 = time_ops.events_time_diff_seconds(df.iloc[0]['timestamp'], df.iloc[1]['timestamp'])
    time_diff_2 = time_ops.events_time_diff_seconds(df.iloc[1]['timestamp'], df.iloc[2]['timestamp'])
    if time_diff_1 <= AUTHORIZE_TIME_THRESHOLD_SECONDS and time_diff_2 <= AUTHORIZE_TIME_THRESHOLD_SECONDS:
        return True
    return False

def future_event_in_time_threshold(df: pd.DataFrame) -> bool: 
    time_diff = time_ops.events_time_diff_seconds(df.iloc[0]['timestamp'], df.iloc[1]['timestamp'])
    if time_diff <= AUTHORIZE_TIME_THRESHOLD_SECONDS:
        return True
    return False   

def closest_surrounding_event_index(df: pd.DataFrame) -> int: 
    time_diff_1 = time_ops.events_time_diff_seconds(df.iloc[0]['timestamp'], df.iloc[1]['timestamp'])
    time_diff_2 = time_ops.events_time_diff_seconds(df.iloc[1]['timestamp'], df.iloc[2]['timestamp'])
    if not surrounding_events_in_time_threshold(df): 
        return -1
    if time_diff_1 < time_diff_2: 
        return 0
    return 2

def assign_transaction_ID_temporally(block_df: pd.DataFrame) -> int:
    if block_df.empty:
        return -1
    if block_df.iloc[0]['event_code'] == code.STARTED: 
        return -2
    if len(block_df) == 2 and \
        block_df.iloc[1]['event_code'] != code.STARTED: 
        return -1
    elif len(block_df) == 3:
        block_df = block_df[1:]
    if future_event_in_time_threshold(block_df) and \
       block_df.iloc[1]['event_code'] == code.STARTED and \
       not pd.isna(block_df.iloc[0]['ID_token']):
        return block_df.iloc[1]['transaction_ID']
    return -1

def get_valid_authorize_surrounding_rows(index: int, df: pd.DataFrame) -> pd.DataFrame: 
    only_index_relevant_df = df[df['device_ID'] == df.loc[index]['device_ID']]
    non_authorize_df = only_index_relevant_df[
                       ((only_index_relevant_df['event_type'] != event_type.REQUEST_START_TRANSACTION_RESPONSE) &
                        (only_index_relevant_df['event_type'] != event_type.AUTHORIZE_RESPONSE) & 
                        (only_index_relevant_df['event_type'] != event_type.STATUS_NOTIFICATION_REQUEST)) |
                       (only_index_relevant_df['timestamp'] == only_index_relevant_df.loc[index]['timestamp'])]
    authorize_positional_index = non_authorize_df.index.get_loc(index)
    if authorize_positional_index == (len(non_authorize_df) - 1):
        return pd.DataFrame()
    if len(non_authorize_df) == 1:
        return pd.DataFrame()
    if authorize_positional_index == 0: 
        return non_authorize_df[authorize_positional_index:authorize_positional_index+2][:]
    else: 
        return non_authorize_df[authorize_positional_index-1:authorize_positional_index+2][:]
    
def assign_transaction_IDs_temporally(index: int, formatted_data_df: pd.DataFrame) -> int: 
    if pd.isna(formatted_data_df.loc[index]['ID_token']):
        return pd.NA
    if not pd.isna(formatted_data_df.loc[index]['transaction_ID']): 
        return formatted_data_df.loc[index]['transaction_ID']
    authorize_block_df = get_valid_authorize_surrounding_rows(index, formatted_data_df)
    transaction_ID = assign_transaction_ID_temporally(authorize_block_df)
    if transaction_ID == -2:
        return transaction_ID
    return pd.NA

def assign_transaction_IDs_credentially(index: int, formatted_data_df: pd.DataFrame) -> str: 
    row = formatted_data_df.loc[index]
    if pd.isna(row['ID_token']):
        return -1
    if row['event_type'] == event_type.REQUEST_START_TRANSACTION_RESPONSE: 
        return row['transaction_ID']
    df_with_token_ID = formatted_data_df[formatted_data_df['ID_token'] == row['ID_token']]
    df_with_token_ID = df_with_token_ID.sort_values(by=['timestamp'])
    no_events_before_timestamp_df = df_with_token_ID[df_with_token_ID['timestamp'] > row['timestamp']]
    for _, matching_row in no_events_before_timestamp_df.iterrows(): 
        time_df = pd.concat([row, matching_row], axis=1).T
        time_diff_seconds = time_ops.events_time_diff_seconds(time_df.iloc[0]['timestamp'], time_df.iloc[1]['timestamp'])
        if (time_diff_seconds < AUTHORIZE_TIME_THRESHOLD_SECONDS) and matching_row['event_code'] == 'Started': 
            return matching_row['transaction_ID']
    only_events_before_timestamp_df = df_with_token_ID[df_with_token_ID['timestamp'] < row['timestamp']]
    for _, matching_row in only_events_before_timestamp_df.iterrows(): 
        time_df = pd.concat([row, matching_row], axis=1).T
        time_diff_seconds = time_ops.events_time_diff_seconds(time_df.iloc[0]['timestamp'], time_df.iloc[1]['timestamp'])
        if (time_diff_seconds < AUTHORIZE_TIME_THRESHOLD_SECONDS) and \
            matching_row['event_code'] == 'Started': 
            return -2
    return -1

def get_transaction_IDs_for_authorizes(formatted_data_df: pd.Series) -> pd.Series: 
    authorize_df = formatted_data_df[formatted_data_df['event_type'] == event_type.AUTHORIZE_RESPONSE]
    formatted_data_df['authorize_transaction_ID'] = authorize_df.index.to_series().apply(assign_transaction_IDs_credentially, 
                                                                        args=(formatted_data_df,))
    formatted_data_df.loc[formatted_data_df['event_type'] == event_type.AUTHORIZE_RESPONSE, 'transaction_ID'] = \
        formatted_data_df[formatted_data_df['event_type'] == event_type.AUTHORIZE_RESPONSE]['authorize_transaction_ID']
    formatted_data_df = formatted_data_df.drop(['authorize_transaction_ID'], axis=1)
    return formatted_data_df

def initialize_formatted_data() -> tuple:
    device_IDs = []
    ID_tokens = []
    transaction_IDs = []
    event_types = []
    event_codes = []
    trigger_reasons = []
    timestamps = []
    response_timestamps = []
    return device_IDs, ID_tokens, transaction_IDs, event_types, event_codes, \
            trigger_reasons, timestamps, response_timestamps

def create_formatted_dataframe(formatted_data: tuple): 
    device_IDs, ID_tokens, transaction_IDs, event_types, \
        event_codes, trigger_reasons, timestamps, response_timestamps = formatted_data
    formatted_df = pd.DataFrame(data={'device_ID': device_IDs, 'ID_token': ID_tokens, 
                                      'transaction_ID': transaction_IDs, 
                                      'event_type': event_types, 'event_code': event_codes, 
                                      'trigger_reason': trigger_reasons, 'timestamp': timestamps, 
                                      'response_timestamp': response_timestamps})
    return formatted_df

def format_data(df: pd.DataFrame) -> pd.DataFrame:
    formatted_attributes = initialize_formatted_data()
    for _, row in df.iterrows():
        if has_relevant_event(row['message']):
            add_status_event(formatted_attributes, row, df)
    formatted_df = create_formatted_dataframe(formatted_attributes)
    formatted_df = formatted_df[formatted_df['event_code'] != 'remove']
    formatted_df = formatted_df.sort_values(by=['timestamp'])
    formatted_df = formatted_df.reset_index(drop=True)
    formatted_df = get_transaction_IDs_for_authorizes(formatted_df)
    formatted_df = formatted_df[formatted_df['transaction_ID'] != -2]
    return formatted_df 

def read_as_json(s: str) -> dict:
    if pd.isna(s): 
        return s
    try: 
        string_json = json.loads(str(s))
    except ValueError: 
        warnings.warn("Warning - Potentially malformed OCPP JSON. Verify that OCPP has properly delimited message values", UserWarning)
        return pd.NA
    return string_json #TODO Change this to just return the string_json(only the message is expected from external) json.loads(string_json['msg'])

if __name__ == "__main__": 
    raw_log_dir = KPI_CALC_REPO_PATH + "/interim-kpi-calculator/data/split_logs"
    formatted_log_dir = KPI_CALC_REPO_PATH + "/interim-kpi-calculator/data/parsed_logs"
    if not os.path.exists(formatted_log_dir): 
        os.mkdir(formatted_log_dir)
    concatenating_dfs = []
    print('------Assembling Formatted Data------')
    for raw_log in tqdm(os.listdir(raw_log_dir)):
        raw_df = pd.read_csv(os.path.join(raw_log_dir, raw_log))
        raw_df['message'] = raw_df['message'].apply(read_as_json)
        raw_df['message_ID'] = raw_df['message'].apply(get_message_ID)
        raw_df['ID_token'] = raw_df['message'].apply(get_ID_token)
        new_df = format_data(raw_df)
        concatenating_dfs.append(new_df)
    new_df = pd.concat(concatenating_dfs)
    new_df.to_csv(os.path.join(formatted_log_dir, "parsed_messages_" + str(datetime.today().strftime('%Y_%m_%d')) + '.csv'), index=False)