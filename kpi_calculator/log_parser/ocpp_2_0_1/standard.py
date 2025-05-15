# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

from kpi_calculator.utils import fraction

STANDARD_THRESHOLD = 0.2
STANDARD_SUB_STRINGS = { 
                        'verbose': ['.cpp:', 'm INFO', 'mTRACE' 'm WARN'], 
                        'explicit':  ['[info]', '[REQUEST]', '[msg-out]', '[msg-in]', 
                                     '[verdict]', '[prompt]', '[api-dismissed]']
                       }

def is_standard(sample_lines: list[str], standard: str) -> str | None: 
    fraction_standard_lines = fraction.AdditiveFraction()
    for line in sample_lines: 
        for sub_string in STANDARD_SUB_STRINGS[standard]: 
            if sub_string in line: 
                fraction_standard_lines.add_to_numerator(1)
        fraction_standard_lines.add_to_denominator(1)   
    if fraction_standard_lines.calculate_fraction() > STANDARD_THRESHOLD:
        return standard
    return None 
