"""Module to encapsulate validation results."""
from abc import ABC


class ValidationResults(ABC):
    """Module to encapsulate validation results."""

    def __init__(self, correctness_df, completeness_df, comparison_df):
        self.correctness_df = correctness_df
        self.completeness_df = completeness_df
        self.comparison_df = comparison_df
