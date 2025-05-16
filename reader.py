# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

import os

from kpi_calculator.log_parser.ocpp_2_0_1 import standard, parser


def read_log(file_path: str, number_sample_lines: int) -> list[str]: 
    lines = []
    with open(file_path, 'r', encoding='utf-8') as infile: 
        for line_number, line in enumerate(infile): 
            if line_number == number_sample_lines: 
                break
            lines.append(line)
        return lines   

def identify_standard(sample_lines: list[str]) -> str:
    standard_error = 'Standard is ambiguous and cannot be inferred from the data provided'
    verbose = standard.is_standard(sample_lines, 'verbose')
    explicit = standard.is_standard(sample_lines, 'explicit')
    if verbose and explicit: 
        raise IOError(standard_error) 
    if explicit:
        return explicit
    if verbose: 
        return verbose
    raise IOError(standard_error)

def set_standard(sample_lines: list[str], preselected_standard: str) -> str: 
    line_standard = preselected_standard
    if line_standard is not None and line_standard != 'explicit' and line_standard != 'verbose': 
        raise ValueError(f"Preselected standard ({preselected_standard}) is not a supported standard")
    if line_standard is None: 
        line_standard = identify_standard(sample_lines)
    return line_standard 

def initialize_parser(file_path: str, preselected_standard: str, number_sample_lines: int) -> parser.LogParser:
    sample_lines = read_log(file_path, number_sample_lines)
    line_standard = set_standard(sample_lines, preselected_standard) 
    log_parser = parser.LogParser(line_standard)       
    return log_parser

def create_log(output_file_path: str) -> None: 
    if os.path.exists(output_file_path): 
        os.remove(output_file_path)
    with open(output_file_path, 'a+', encoding='utf-8') as outfile: 
        outfile.write('device_ID,message,timestamp\n')

def parse_logs(log_dir_path: str, output_file_path: str, preselected_standard: str = None, number_sample_lines: int = 100): 
    if not os.path.exists(log_dir_path):
        os.mkdir(log_dir_path)
    sample_log_path = os.listdir(log_dir_path)[0]
    log_parser = initialize_parser(os.path.join(log_dir_path, sample_log_path), preselected_standard, number_sample_lines)
    create_log(output_file_path)
    for device_ID, log_file_path in enumerate(os.listdir(log_dir_path)): 
        log_parser.parse_log(os.path.join(log_dir_path, log_file_path), output_file_path, device_ID)
        
if __name__ == "__main__": 
    log_dir_path = r'insert/path/to/ocpp/log/data'
    output_file_path = r'/insert/path/to/repo/interim-kpi-calculator/data/filtered_format'
    parse_logs(log_dir_path, output_file_path)
