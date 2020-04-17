"""This module exposes the handler class for the dataframes validation process."""
import datetime

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from spark_validation.common.constants import Constants
from spark_validation.common.general_validator import GeneralDFValidator


class DataframeValidator(GeneralDFValidator):
    """Class to create a handler with the main function for the grid ingestion process."""

    def __init__(
        self,
        spark,
        source_df,
        id_col_name,
        correctness_rules_dict,
        parent_children_validation_pairs,
        completeness_rules_dic,
        comparable_dfs_list,
    ):
        """Create handler with initial df for the specific date."""
        self.spark = spark
        self.source_df = source_df
        self.id_col_name = id_col_name
        self.correctness_rules_dict = correctness_rules_dict
        self.parent_children_validation_pairs = parent_children_validation_pairs
        self.completeness_rules_dic = completeness_rules_dic
        self.comparable_dfs_list = comparable_dfs_list

    def process(self):
        """Run the the entire validation pipeline.

        1. Run all the rules for correcteness.
        2. Run all the rules completness.
        3. Return processed_df with all the computed values.
        """
        processed_df = self.source_df.transform(
            lambda df: self.join_cols_with_all_parents(
                df, self.parent_children_validation_pairs
            )
        ).transform(
            lambda df: self.build_correctness_df(
                df, self.correctness_rules_dict, self.parent_children_validation_pairs
            )
        )

        validation_result_cols = list(
            filter(lambda x: Constants.IS_ERROR_COL in x, processed_df.schema.names)
        )
        processed_df = processed_df.select(
            *([self.id_col_name] + validation_result_cols)
        )

        processed_df = processed_df.transform(
            lambda df: self.build_correctness_report_df(df, validation_result_cols)
        ).transform(
            lambda df: self.build_computed_rules_correctness_df(
                df, self.completeness_rules_dic
            )
        )
        return processed_df

    def compare(self):
        """Compare the source df with related dfs.

        Get comparison metrics like:
        1. missing_cols_right.
        2. missing_cols_left.
        3. missing_vals_right.
        4. missing_vals_left.
        """
        return self.spark.createDataFrame(
            self.compared_with_related_dfs(
                self.source_df, self.id_col_name, self.comparable_dfs_list
            ),
            Constants.OUTPUT_COMPARABLE_COLS,
        ).withColumn(Constants.DATE_TIME_REPORT_COL, F.lit(datetime.datetime.now()))


DataFrame.transform = DataframeValidator.transform
