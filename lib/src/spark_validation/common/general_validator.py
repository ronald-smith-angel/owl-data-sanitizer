"""This module exposes a general interface with common df functions across all the pipelines.

This function could be extensible to create specific handlers. For instance: PandasDataHandler(GeneralDFHandler).

"""
import datetime
from abc import ABC
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from spark_validation.common.constants import Constants


class GeneralDFValidator(ABC):
    """Class with general handlers functions."""

    def transform(self, f):
        """Wrap the transform spark function non available for python."""
        return f(self)

    @staticmethod
    def rename_cols(df, transformation_map):
        """Rename a set of spark columns within a df using a transformation_map dictionary.

        Example:
        df:
            +--------+--------+
            |   col1 |   col2 |
            |--------+--------+
            |     15 |     76 |
            |     30 |     97 |
            +--------+--------+
        transformation_map :
            {col1: id, col2: code}
        return:
            +--------+--------+
            |   id   |   code |
            |--------+--------+
            |     15 |     76 |
            |     30 |     97 |
            +--------+--------+
        """
        return reduce(
            lambda internal_df, col_name: internal_df.withColumnRenamed(
                col_name, transformation_map[col_name]
            ),
            transformation_map.keys(),
            df,
        )

    @staticmethod
    def combine_dataframes(sources):
        """Join multiple dataframes using the spark union function."""
        return reduce(lambda x, y: x.union(y), sources)

    @staticmethod
    def join_cols_with_all_parents(df, parent_validations_pairs):
        """Join df with all parent ids to obtain incorrect ids."""
        for col, parent in parent_validations_pairs:
            df = GeneralDFValidator.join_grid_with_parent(df, col, parent)
        return df

    @staticmethod
    def join_grid_with_parent(df, id_col, parent_id_col):
        """Join a df with a single parent id to obtain incorrect ids."""
        # Renaming col to avoid spark duplicate cols issues.
        _ids_renamed_org_id = id_col + "_" + parent_id_col
        parent_ids_df = df.select(id_col).withColumnRenamed(id_col, _ids_renamed_org_id)

        self_joined_df = df.join(
            parent_ids_df,
            df[parent_id_col] == parent_ids_df[_ids_renamed_org_id],
            "left",
        )

        return self_joined_df

    @staticmethod
    def _validate_parent_id(df, id_col, parent_id_col):
        # Generating proper parent id validation column obtained after join_grid_with_parent = prefix + col_name.
        _ids_renamed_org_id = id_col + "_" + parent_id_col
        return df.withColumn(
            Constants.IS_ERROR_COL + _ids_renamed_org_id,
            F.when(
                (
                    (F.col(_ids_renamed_org_id).isNotNull())
                    | (F.col(parent_id_col).isNull())
                ),
                0,
            ).otherwise(1),
        )

    @staticmethod
    def build_correctness_report_df(processed_df, validated_cols):
        """Build a report df computing column errors.

        1. Sum of errors per column.
        2. Add an over all row count.
        3. Add a time stamp to this dataframe.
        """
        windows_errors = Window.partitionBy(Constants.DATE_TIME_REPORT_COL)
        report_df = reduce(
            lambda internal_df, col_name: internal_df.transform(
                lambda df: df.withColumn(
                    col_name + Constants.SUM_REPORT_SUFFIX,
                    F.sum(col_name).over(windows_errors),
                )
            ),
            validated_cols,
            processed_df.withColumn(
                Constants.DATE_TIME_REPORT_COL, F.lit(datetime.datetime.now())
            ),
        ).withColumn(
            Constants.OVER_ALL_COUNT_COL,
            F.count(Constants.DATE_TIME_REPORT_COL).over(windows_errors),
        )

        return report_df

    @staticmethod
    def build_computed_rules_correctness_df(processed_df, rules_map):
        """Build a dataframe with some rules computed.

        :param processed_df: input dataframe.
        :param rules_map: a map of rules with format {col_name = spark_sql_expr}
        :return: a dataframe with a new column IS_ERROR (1 - ERROR or 0 - NO ERROR) per column on the map.
        """

        return reduce(
            lambda internal_df, col_name: internal_df.transform(
                lambda df: GeneralDFValidator._compute_col_val_correctness(
                    df, col_name, rules_map[col_name]
                )
            ),
            rules_map.keys(),
            processed_df,
        )

    @staticmethod
    def build_correctness_df(
        processed_df, validation_rules_map, parent_validations_pairs
    ):
        """Build correctness df.

        1. validate all the rules per column.
        2. return df with error columns. This column will have the following schema:
        col_name = Constants.IS_ERROR_COL + col_name.
        value = 1 when error, 0 when column is clean.
        3. Add a column with Constants.IS_ERROR_COL + Constants.ROW_ERROR_SUFFIX representing error on any
        column of the row.
        """
        _list_correctness_cols = list(validation_rules_map.keys())
        _list_cols_parent_validation = list(
            set([pair[0] + "_" + pair[1] for pair in parent_validations_pairs])
        )
        _list_cols_parent_cols = list(
            set([pair[0] for pair in parent_validations_pairs])
        )
        _error_cols_correctness = list(
            map(lambda c: Constants.IS_ERROR_COL + c, validation_rules_map.keys(),)
        )
        _error_cols_parents_pairs = list(
            map(lambda c: Constants.IS_ERROR_COL + c, _list_cols_parent_validation,)
        )
        _list_general_rows_errors = list(
            [Constants.IS_ERROR_COL + Constants.ROW_ERROR_SUFFIX]
        )

        final_select_cols = (
            _list_correctness_cols
            + _list_cols_parent_validation
            + _list_cols_parent_cols
            + _error_cols_correctness
            + _error_cols_parents_pairs
            + _list_general_rows_errors
        )

        validate_expr = GeneralDFValidator._generate_validate_errors_expr(
            _list_correctness_cols + _list_cols_parent_validation
        )
        validated_df = GeneralDFValidator.build_computed_rules_correctness_df(
            processed_df, validation_rules_map
        )

        validated_df = (
            reduce(
                lambda internal_df, pair_parent: internal_df.transform(
                    lambda df: GeneralDFValidator._validate_parent_id(
                        df, pair_parent[0], pair_parent[1]
                    )
                ),
                parent_validations_pairs,
                validated_df,
            )
            .withColumn(
                Constants.IS_ERROR_COL + Constants.ROW_ERROR_SUFFIX,
                F.when(F.expr(validate_expr), 1).otherwise(0),
            )
            .select(final_select_cols)
        )
        return validated_df

    @staticmethod
    def _compute_col_val_correctness(df, col_name, col_rule):
        # Error column name is generated with error_prefix + col_name.
        return df.withColumn(
            Constants.IS_ERROR_COL + col_name, F.when(F.expr(col_rule), 0).otherwise(1),
        )

    @staticmethod
    def _generate_validate_errors_expr(list_validation_cols):
        """Generate SQL exp that validates that there a not error (col_val == 1) on any validation column."""
        return """{}{} == 1 {}""".format(
            Constants.IS_ERROR_COL,
            list_validation_cols[0],
            "".join(
                list(
                    map(
                        lambda x: " or {}{} == 1".format(Constants.IS_ERROR_COL, x),
                        list_validation_cols[1:],
                    )
                )
            ),
        )

    @staticmethod
    def compared_with_related_dfs(source_df, id_col_name, map_related_dfs):
        """Compare source df with related dfs.

        Obtaining a list per related dfs:
        1. Columns present in source not in related.
        2. Columns present in related in source.
        When both previous are empty:
        3. Row values present in source not equal in related.
        4. Row values present in related not equal in source.
        """
        comparison_results = []
        for k, df in map_related_dfs:
            missing_cols_right = GeneralDFValidator._missing_values_between_schemas(
                source_df.schema, df.schema
            )
            missing_cols_left = GeneralDFValidator._missing_values_between_schemas(
                df.schema, source_df.schema
            )

            missing_vals_right = GeneralDFValidator._list_different_rows_ids_between_dfs(
                source_df, id_col_name, df, missing_cols_right
            )

            missing_vals_left = GeneralDFValidator._list_different_rows_ids_between_dfs(
                df, id_col_name, source_df, missing_cols_left
            )

            comparison_results.append(
                (
                    k,
                    ",".join(missing_cols_right),
                    ",".join(missing_cols_left),
                    ",".join(missing_vals_right),
                    ",".join(missing_vals_left),
                )
            )

        return comparison_results

    @staticmethod
    def _missing_values_between_schemas(schema1, schema2):
        return list(
            set(list(map(lambda c: c.name + ":" + c.dataType.simpleString(), schema1)))
            - set(
                list(map(lambda c: c.name + ":" + c.dataType.simpleString(), schema2))
            )
        )

    @staticmethod
    def _list_different_rows_ids_between_dfs(
        source_df, id_col_name, related_df, schema_correct
    ):
        return (
            list(
                map(
                    lambda col: col.__getitem__(id_col_name),
                    source_df.subtract(related_df).select(id_col_name).collect(),
                )
            )
            if not schema_correct
            else []
        )


DataFrame.transform = GeneralDFValidator.transform
