# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

from dataclasses import dataclass 


@dataclass
class AdditiveFraction:

    numerator: int = 0
    denominator: int = 0

    def __repr__(self) -> str:
        return f"Numerator: {self.numerator}, Denominator: {self.denominator}, Fraction: {self.calculate_fraction()}"

    def add_to_numerator(self, add_value: int) -> None: 
        self.numerator += add_value

    def add_to_denominator(self, add_value: int) -> None: 
        self.denominator += add_value

    def calculate_fraction(self) -> float | str: 
        if self.denominator == 0: 
            return 'undefined'
        return float(self.numerator) / float (self.denominator)