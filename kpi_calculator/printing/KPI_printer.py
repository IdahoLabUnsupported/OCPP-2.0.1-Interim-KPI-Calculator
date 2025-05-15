# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

from __future__ import annotations

import xlsxwriter
import xlsxwriter.worksheet 

class KPIExcelWriter: 
    
    def __init__(self, KPI_file_name: str): 
        self._book = xlsxwriter.Workbook(KPI_file_name)
        
    def _write_header(self, sheet: Worksheet, column_b_name: str, column_c_name: str, KPI_name: str) -> None: 
        sheet.write('A1', 'equation')
        sheet.write('B1', column_b_name)
        sheet.write('C1', column_c_name)
        sheet.write('D1', 'percent_contribution')
        sheet.write('E1', KPI_name)
        
    def _write_data_line(self, sheet: Worksheet, row: str, equation_number: int, equation: fraction.AdditiveFraction,
                         KPI_value: str, percent_contribution: float) -> None:
        sheet.write('A' + row, equation_number)
        sheet.write('B' + row, equation.numerator)
        sheet.write('C' + row, equation.denominator)
        sheet.write('D' + row, percent_contribution)
        sheet.write('E' + row, KPI_value)
        
    def write_percentage_based_KPI_sheet(self, interim_KPIs: InterimKPIs, column_b_header: str, column_c_header: str, 
                                         KPI_name: str, relevant_equations: list[int], exclude_aggregate: bool = False) -> None:
        sheet = self._book.add_worksheet(KPI_name)
        self._write_header(sheet, column_b_header, column_c_header, KPI_name)
        for index, equation in enumerate(relevant_equations): 
            self._write_data_line(sheet, str(index + 2), equation, interim_KPIs.equation(equation),
                                  interim_KPIs.KPI_value(equation), 
                                  interim_KPIs.percent_contribution_for_x_KPI(equation, KPI_name))
        if not exclude_aggregate: 
            sheet.write('A6', 'Total Samples')
            sheet.write('B6', interim_KPIs.total_numerator_for_x_KPI(KPI_name))
            sheet.write('C6', interim_KPIs.total_denominator_for_x_KPI(KPI_name))
            sheet.write('D8', 'Weighted Average')
            sheet.write('E8', interim_KPIs.weighted_average_for_x_KPI(KPI_name))        

    def write_charge_start_time(self, interim_KPIs: InterimKPIs) -> None:
        if interim_KPIs.num_charge_start_time_samples() == 0: 
            warnings.warn("Warning: No charge start times for dataset. KPI for charge start not calculated.")
            return
        sheet = self._book.add_worksheet('charge_start_time')
        sheet.write('A1', 'equation')
        sheet.write('A2', '9')
        sheet.write('B1', '10th_percentile')
        sheet.write('C1', '25th_percentile')
        sheet.write('D1', '50th_percentile')
        sheet.write('E1', '75th_percentile')
        sheet.write('F1', 'total_samples')
        sheet.write('B2', interim_KPIs.x_percentile_charge_start_time(10))
        sheet.write('C2', interim_KPIs.x_percentile_charge_start_time(25))
        sheet.write('D2', interim_KPIs.x_percentile_charge_start_time(50))
        sheet.write('E2', interim_KPIs.x_percentile_charge_start_time(75))
        sheet.write('F2', interim_KPIs.num_charge_start_time_samples())
        
    def write_KPIs(self): 
        self._book.close()