# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

from dataclasses import dataclass
        
def write_parsed_log_line(output_file_path: str, message: str, device_ID: int, date: str) -> None:
    message = message.replace("\"", "\"\"")
        
    with open(output_file_path, 'a+', encoding='utf-8') as outfile: 
        outfile.write(str(device_ID) + ',' + "\"" + message + "\"" + ',' + date + '\n')

@dataclass
class StandardSubstrings: 
    
    message_in_substring: str
    message_out_substring: str
    
@dataclass 
class StandardDateStringPos:

    date_pos_start: int
    date_pos_end: int

class LineParser:
    
    def __init__(self, standard: str):
        self._standard_substrings = STANDARD_SUBSTRINGS[standard]
        self._standard_date_string_pos = STANDARD_DATE_STRING_POS[standard]
        
    def relevant_substring(self, line: str) -> str | None: 
        if self._standard_substrings.message_in_substring in line: 
            return self._standard_substrings.message_in_substring
        elif self._standard_substrings.message_out_substring in line: 
            return self._standard_substrings.message_out_substring
        return None
    
    def parse_message(self, line: str) -> str | None: 
        indicator = self.relevant_substring(line)
        if indicator is None: 
            return None
        index_of_message_start = line.find(indicator) + (len(indicator) - 1)
        message = line[index_of_message_start:]
        return message    

    def parse_date(self, line: str) -> str:
        return line[self._standard_date_string_pos.date_pos_start: self._standard_date_string_pos.date_pos_end]   
    
STANDARD_SUBSTRINGS = {
                        'explicit' : StandardSubstrings('[msg-in] [', '[msg-out] ['), 
                        'verbose' : StandardSubstrings('>>> [', '<<< [') 
                        } 

STANDARD_DATE_STRING_POS = {
                        'explicit' : StandardDateStringPos(1, 12),
                        'verbose' : StandardDateStringPos(0, 24)
                    }

class LogParser :
    
    def __init__(self, standard: str): 
        self._line_parser = LineParser(standard)

    def parse_log(self, log_file_path: str, output_file_path: str, device_ID: int) -> None:
        with open(log_file_path, 'r', encoding='utf-8') as infile: 
            for line in infile: 
                if self._line_parser.relevant_substring(line) is None:
                    continue
                message = self._line_parser.parse_message(line)
                message = message[:-1]
                date = self._line_parser.parse_date(line)
                write_parsed_log_line(output_file_path, message, device_ID, date)
