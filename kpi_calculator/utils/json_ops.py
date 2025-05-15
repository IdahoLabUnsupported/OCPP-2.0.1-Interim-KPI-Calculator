# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

import json

# function assumes only one, nested braced group in a string

def string_to_JSON(string: str) -> dict | None:
    string = string.replace('\n', '')
    string = r'{}'.format(string)
    if string[0] != '{' or string[-1] != '}':
        return None
    open_brace_index = string.index('{')
    close_brace_index = -1
    json_str = string[open_brace_index:close_brace_index]
    if json_str == '{}': 
        return None
    json_object = json.loads(json_str)
    return json_object