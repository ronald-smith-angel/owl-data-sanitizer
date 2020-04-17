"""Specific general constants used across the validation pipeline."""


class Constants:
    """Class with Constants for input and output values used in the the validation pipeline."""

    DATE_TIME_REPORT_COL = "dt"
    SUM_REPORT_SUFFIX = "_SUM"
    OVER_ALL_COUNT_COL = "OVER_ALL_COUNT"
    IS_ERROR_COL = "IS_ERROR_"
    ROW_ERROR_SUFFIX = "_ROW"
    RULES_REPORT_SUFFIX = "_rules_report"
    COMPARISON_REPORT_SUFFIX = "_comparison_report"
    REPORT_DF_COL = "df"
    MISSING_COLS_RIGHT_COL = "missing_cols_right"
    MISSING_VALS_RIGHT_COL = "missing_vals_right"
    MISSING_COLS_LEFT_COL = "missing_cols_left"
    MISSING_VALS_LEFT_COL = "missing_vals_left"
    OUTPUT_COMPARABLE_COLS = [
        REPORT_DF_COL,
        MISSING_COLS_RIGHT_COL,
        MISSING_COLS_LEFT_COL,
        MISSING_VALS_RIGHT_COL,
        MISSING_VALS_LEFT_COL,
    ]
