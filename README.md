# OCPP 2.0.1 Interim KPI Calculator

This project was designed to take OCPP 2.0.1 log data and calculate the aggregate KPIs for the given data. 

## Description

The project is split into four pieces that need to be ran sequencially. The first is a raw OCPP log parser. The second is a file splitter. The third is a message parser. The final piece is the Interim KPI calculator. 

### OCPP Log Parser

reader.py

The OCPP log parser was created from two different formats of raw OCPP 2.0.1 data. Its intended purpose is to extract device IDs and OCPP event messages from nontabular text logs. The parser looks for specific substrings in the logs to identify which of the two "standards" it should select from. It is possible that your OCPP 2.0.1 log format is not compatible with the parser. It is then recommended to parse your own data into the tabular format provided below. For example, if the OCPP log you have doesn't contain a date, you will need to add a date to the timestamp. Refer to the the example file included in the data folder:  "/interim-kpi-calculator/data/raw_ocpp_logs/example_log.log".

This format is the result of the OCPP log parser's processes.    

device_ID | message | timestamp
--- | --- | ---
1 | "[2, "05fe190e-1e0b-4dfa-b6c0-c490455e97ac", "GetVariables", {"getVariableData":[{"component":{"name":"TxCtrlr"},"variable":{"name":"TxStartPoint"}}]}]" | "2024-09-27T13:45:48.687Z"
1 | "[3, "05fe190e-1e0b-4dfa-b6c0-c490455e97ac", {"getVariableResult": [{"attributeStatus": "Accepted", "attributeValue": "PowerPathClosed", "component": {"name": "TxCtrlr"}, "variable": {"name": "TxStartPoint"}}]}]" | "2024-09-27T13:54:45.106Z"
2 | "[2, "06ec54f6-a79b-45ba-89a0-9e8ea6938dd2", "GetVariables", {"getVariableData":[{"component":{"name":"TxCtrlr"},"variable":{"name":"TxStopPoint"}}]}]" | "2024-09-26T16:28:25.739Z"
2 | "[3, "06ec54f6-a79b-45ba-89a0-9e8ea6938dd2", {"getVariableResult": [{"attributeStatus": "Accepted", "attributeValue": "PowerPathClosed", "component": {"name": "TxCtrlr"}, "variable": {"name": "TxStopPoint"}}]}]" | "2024-09-26T16:33:26.241Z"

### File Splitter

split_data_into_charger_files.py 

The KPI generator does not perform any of its calculations in parallel. Instead, we opt for a naive batching approach. The splitter takes the file generated from the parser and creates many smaller files for each of the device IDs in the dataset. This allows the pandas queries in the log formatter to be iterate over a significantly smaller slice of data, increasing performance significantly. It is possible to just iterate over the unique device IDs in the log formatter, but we recommend keeping this approach \(especially if the data are many GBs large\) to allow for rapid re-execution. 

### Message Parser

parse_messages.py

This step takes messages from each of the files (containing distinct device IDs) and breaks the message out into pieces. The final result is a file with different columns specifying different attributes of the JSON message. The file is an aggregation of all different devices. This is the most complex portion of the code. The example below shows a single expected transaction for a single device. **IMPORTANT: The nomenclature used within this file does not map cleanly back to OCPP 2.0.1.** The message parser is intended to extract relevant data and format it in ways that are easy for the Interim KPI calculator to extract and sum. For example, *Accepted* is not a valid trigger reason in OCPP 2.0.1 but it is in the formatted file as a response. 

**IMPORTANT: If your input file does not contain well formatted JSON as input, a warning will emerge. It is common for OCPP JSON to be malformed but if every line is malformed then there is an issue. If you suspect that your JSON is being well read then you may disable the warning.** 


device_ID | ID_token | transaction_ID | event_type | event_code | trigger_reason | timestamp | response_timestamp
--- | --- | --- | --- | --- | --- | --- | --- 
1 | 0001 | 0001 | TransactionEvent | Started | Accepted | 2025-27-01T02:36:42Z | 2025-27-01T02:36:53Z
1 |  | 0001 | StatusNotificationEvent | Occupied |  | 2025-27-01T02:37:22Z | 
1 |  | 0001 | TransactionEvent | Charging | ChargingStateChanged | 2025-27-01T02:37:53Z | 
1 |  | 0001 | TransactionEvent | SuspendedEV | ChargingStateChanged | 2025-27-01T02:48:02Z | 
1 |  | 0001 | TransactionEvent | SuspendedEVSE | ChargingStateChanged | 2025-27-01T02:48:04Z | 
1 |  | 0001 | TransactionEvent | Ended | StoppedByEV | 2025-27-01T02:55:40Z | 

### KPI Calculator

calculator.py

The KPI calculator takes the parsed messages, as a single file, and calculates the KPI from that data. An excel file is produced with four sheets. These contain the metrics for Session Success, Charge Start Success, Charge End Success, and Charge Start Time. It includes the metrics for the different equations in the Interim KPI Implementation Guide as well as a weighted sum of the different equations for each KPI (excluding Charge End Success and Charge Start Time). 

***To run this script you must identify a parsed input file and also include the start and end range in the CLI command as arguments or change the input file on line 21 and the date ranges on lines 19 & 20.***


## Dependencies

* python 3.12
* numpy 2.1.3
* pandas 2.2.3
* tqdm 4.66.6
* xlsxwriter 3.2.0

## Executing program

Ensure the KPI_CALC_REPO_PATH is set proper in reader.py, split_data_into_charger_files.py, parse_messages.py, and calculator.py.

The following commands are meant to be executed in order: 

python reader.py (if your OCPP conforms to the standards present in the code)
python split_data_into_charger_files.py
python parse_messages.py
python calculator.py --start_date <string of the start date of the data> --end_date <string of the end date of the date> --pf <string of parsed file used as input for calculations>

## Assumptions

The implementation guide cannot answer for all the edge cases that arise from the practical realities of logging data. Here, we list some of the assumptions that we took in order to calculate the KPIs

* When looking for responses to request codes, we only look forward, assuming that the timestamps are correct and responses should only be logged post-request. We assume the first response with a message or token ID matching the request is the desired response.
* When binding Authorizes to TransactionEvents with "Start" as their event_code, we initially look forward with a window of 300 seconds. We search for matching ID token in that window. If none is found we look backwards (same time window).
* If an Authorize is not bound to an event, it is an orphan event. 
* If a RequestStart has no matching transaction ID, it is an orphan event. 
* Combination of status/transactionId attributes assumed to only exist for RequestStartResponses, not RequestStopResponses 
* idTokenInfo attribute assumed to only exist for Authorizes and not RequestStarts/RequestStops
* Assuming OCPP message indices exist as enumerated in *message.py*. 
* A transaction must have a valid start condition (as enumerated in sections 3.2.1 and 3.2.3 in the Interim KPI Implementation Guide) in order to record a PowerDeliveryAttempt. This prevents potential values being greater than 1 or divide by 0 errors..
* A transaction must have both a T<sub>*power*</sub> and a T<sub>*attempt*</sub> to record a transaction's Charge Start Time
* A transaction must have a PowerDeliveryAttempt to record a TransactionEventRequest with a valid stop trigger reason for Charge End Success. This prevents potential values being greater than 1 or divide by 0 errors. 
* A transaction must have a valid start condition (as enumerated in sections 3.5.1 and 3.5.3 in the Interim KPI Implementation Guide) in order to record a TransactionEventRequest with a valid stop trigger reason for Session Success. This prevents potential values being greater than 1 or divide by 0 errors.  

## Authors

Paden Rumsey (paden.rumsey@inl.gov)
Casey Quinn (casey.quinn@inl.gov)

## Version History

* 1.0 initial release

## License

This project is licensed under the MIT License - see the LICENSE file for details

Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

## Acknowledgments

This calculator is developed around the Interim KPIs defined by the [ChargeX Consortium](https://inl.gov/chargex/) and detailed in the following documents:

1. Quinn, Casey W., et al. "Customer-Focused Key Performance Indicators for Electric Vehicle Charging." , Jun. 2024. https://doi.org/10.2172/2377347
2. Savargaonkar, Mayuresh, et al. "Implementation Guide of Customer-Focused Key Performance Indicators for Electric Vehicle Charging." , Dec. 2024. https://doi.org/10.2172/2513383

