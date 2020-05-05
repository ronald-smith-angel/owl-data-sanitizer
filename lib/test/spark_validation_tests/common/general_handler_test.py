"""Module with general function tests for the GeneralDFHandler."""
import os
import sys
import unittest

import pyspark.sql.functions as F

from spark_validation.common.config import Config
from spark_validation.common.constants import Constants
from spark_validation.dataframe_validation import file_system_validator
from spark_validation.dataframe_validation import hive_validator
from spark_validation.dataframe_validation.dataframe_validator import DataframeValidator
from spark_validation_tests.common.pyspark_test import PySparkTest

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))


class GeneralHandlerTest(PySparkTest):
    """Class with general function tests for the GeneralDFHandler."""

    TEST_DATABASE_NAME = "test"

    def setUp(self):
        """Init test db for grid."""
        self.spark.sql(
            "CREATE DATABASE IF NOT EXISTS {}".format(
                GeneralHandlerTest.TEST_DATABASE_NAME
            )
        )

    @classmethod
    def setUpClass(cls):
        """Init the shared values for the tests."""
        super(GeneralHandlerTest, cls).setUpClass()
        cls.spark.sql(
            "CREATE DATABASE IF NOT EXISTS {}".format(
                GeneralHandlerTest.TEST_DATABASE_NAME
            )
        )

        cls.source_df = cls._create_source_df(
            PACKAGE_DIR + "/mock_data/data_sample.csv"
        )
        cls.grid_diff_df = cls._create_source_df(
            PACKAGE_DIR + "/mock_data/data_sample_diff.csv"
        )
        cls.grid_diff_2_df = cls._create_source_df(
            PACKAGE_DIR + "/mock_data/data_sample_diff_2.csv"
        )

    @classmethod
    def _create_source_df(cls, csv_file):
        return (
            cls.spark.read.option("delimiter", ",")
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "PERMISSIVE")
            .csv(csv_file)
        )

    def test_grid_validator_process(self):
        """Integration test for rule set defined in mock config file."""
        test_rules = {
            "CODE": """CODE is not null and CODE != '' and CODE != 'null'""",
            "NAME": """NAME is not null and NAME != '' and NAME != 'null'""",
            "GENERAL_ID": (
                "GENERAL_ID is not null and GENERAL_ID != '' and GENERAL_ID != 'null' and"
                " CHAR_LENGTH(GENERAL_ID) < 4"
            ),
            "ULTIMATE_PARENT_ID": """ULTIMATE_PARENT_ID is not null""",
            "PARENT_ID": """PARENT_ID is not null""",
        }

        parent_rules = [
            ("GENERAL_ID", "ULTIMATE_PARENT_ID"),
            ("GENERAL_ID", "PARENT_ID"),
        ]

        completeness_rules = {"OVER_ALL_COUNT": """OVER_ALL_COUNT <= 7"""}

        validator = DataframeValidator(
            spark=self.spark,
            source_df=self.source_df,
            id_col_name="GENERAL_ID",
            correctness_rules_dict=test_rules,
            parent_children_validation_pairs=parent_rules,
            completeness_rules_dic=completeness_rules,
            comparable_dfs_list=[
                ("diff_df", self.grid_diff_df),
                ("diff_df_2", self.grid_diff_2_df),
            ],
        )

        processed_df = validator.process()

        comparable_df = validator.compare()

        self.assertEqual(processed_df.count(), 8)
        self.assertEqual(comparable_df.count(), 2)

    def test_integration_hive_validator(self):
        """Integration test for rule set defined in mock config file."""
        with open(PACKAGE_DIR + "/mock_data/config_example.json") as f:
            config = Config.parse(f)

        self.source_df.write.saveAsTable(config.source_df)
        self.grid_diff_df.write.saveAsTable(config.comparable_dfs_list[0])
        self.grid_diff_2_df.write.saveAsTable(config.comparable_dfs_list[1])

        source_read_df = self.spark.table(config.source_df)
        comparable_dfs_list = [
            (t, self.spark.table(t)) for t in config.comparable_dfs_list
        ]

        validator = DataframeValidator(
            spark=self.spark,
            source_df=source_read_df,
            id_col_name=config.id_col_name,
            correctness_rules_dict=config.correctness_rules_dict,
            parent_children_validation_pairs=config.parent_children_validation_pairs,
            completeness_rules_dic=config.completeness_rules_dic,
            comparable_dfs_list=comparable_dfs_list,
        )

        processed_df = validator.process()
        comparable_df = validator.compare()

        self.assertEqual(processed_df.count(), 8)
        self.assertEqual(comparable_df.count(), 2)

        self.spark.sparkContext.addFile(PACKAGE_DIR + "/mock_data/config_example.json")
        sys.argv = ["example.py", "-c", PACKAGE_DIR + "/mock_data/config_example.json"]

        hive_validator.init()

        correctness_table = self.spark.table(config.output_correctness_table)
        completeness_table = self.spark.table(config.output_completeness_table)
        comparison_table = self.spark.table(config.output_comparison_table)

        # Correctness validations.
        _is_error_name = Constants.IS_ERROR_COL + "NAME" + Constants.SUM_REPORT_SUFFIX
        _sum_errors_col = (
            Constants.IS_ERROR_COL
            + Constants.ROW_ERROR_SUFFIX
            + Constants.SUM_REPORT_SUFFIX
        )
        self.assertEqual(correctness_table.count(), 8)

        self.assertEqual(
            correctness_table.select(_is_error_name).first()[_is_error_name], 4
        )
        self.assertEqual(
            correctness_table.select(_sum_errors_col).first()[_sum_errors_col], 5
        )

        # Completeness validations.
        _is_error_count_over_all = Constants.IS_ERROR_COL + Constants.OVER_ALL_COUNT_COL
        self.assertEqual(
            completeness_table.select(_is_error_count_over_all).first()[
                _is_error_count_over_all
            ],
            1,
        )

        # Comparison validations.

        self.assertEqual(
            comparison_table.filter(
                F.col(Constants.REPORT_DF_COL) == config.comparable_dfs_list[0]
            )
            .select(Constants.MISSING_VALS_RIGHT_COL)
            .first()[Constants.MISSING_VALS_RIGHT_COL],
            "6,7",
        )

        self.assertEqual(
            comparison_table.filter(
                F.col(Constants.REPORT_DF_COL) == config.comparable_dfs_list[1]
            )
            .select(Constants.MISSING_COLS_LEFT_COL)
            .first()[Constants.MISSING_COLS_LEFT_COL],
            "GENERAL_ID:int",
        )

    def test_integration_fs_validator(self):
        """Integration test for rule set defined in mock config file."""
        with open(PACKAGE_DIR + "/mock_data/config_example_local.json") as f:
            config = Config.parse(f)

        config.source_df = PACKAGE_DIR + config.source_df
        config.output_correctness_table = PACKAGE_DIR + config.output_completeness_table
        config.output_completeness_table = (
            PACKAGE_DIR + config.output_completeness_table
        )
        config.output_comparison_table = PACKAGE_DIR + config.output_comparison_table
        config.comparable_dfs_list = list(
            map(lambda x: PACKAGE_DIR + x, config.comparable_dfs_list)
        )

        self.spark.sparkContext.addFile(
            PACKAGE_DIR + "/mock_data/config_example_local.json"
        )
        sys.argv = [
            "example.py",
            "-c",
            PACKAGE_DIR + "/mock_data/config_example_local.json",
        ]

        file_system_validator.init()

        correctness_table = self.spark.read.json(
            "/tmp/mock_data/output/data_sample_test_correctness"
        )
        completeness_table = self.spark.read.json(
            "/tmp/mock_data/output/data_sample_test_completeness"
        )
        comparison_table = self.spark.read.json(
            "/tmp/mock_data/output/data_sample_test_comparison"
        )

        self.assertTrue(correctness_table.count() >= 8)
        self.assertTrue(completeness_table.count() >= 1)
        self.assertTrue(comparison_table.count() >= 1)

    def test_sample_case_integration_fs_validator(self):
        """Integration test for rule set defined in mock config file."""
        with open(PACKAGE_DIR + "/mock_data/config_family_fs.json") as f:
            config = Config.parse(f)

        config.source_df = PACKAGE_DIR + config.source_df
        config.output_correctness_table = PACKAGE_DIR + config.output_completeness_table
        config.output_completeness_table = (
            PACKAGE_DIR + config.output_completeness_table
        )
        config.output_comparison_table = PACKAGE_DIR + config.output_comparison_table
        config.comparable_dfs_list = list(
            map(lambda x: PACKAGE_DIR + x, config.comparable_dfs_list)
        )

        self.spark.sparkContext.addFile(
            PACKAGE_DIR + "/mock_data/config_family_fs.json"
        )
        sys.argv = [
            "example.py",
            "-c",
            PACKAGE_DIR + "/mock_data/config_family_fs.json",
        ]

        file_system_validator.init()

        correctness_table = self.spark.read.json(
            "/tmp/mock_data/output/family_sample_test_correctness"
        )
        completeness_table = self.spark.read.json(
            "/tmp/mock_data/output/family_sample_test_completeness"
        )
        comparison_table = self.spark.read.json(
            "/tmp/mock_data/output/family_sample_test_comparison"
        )

        self.assertTrue(correctness_table.count() >= 6)
        self.assertTrue(completeness_table.count() >= 1)
        self.assertTrue(comparison_table.count() >= 1)

    @classmethod
    def tearDownClass(cls):
        """Remove spark tables for testing."""
        cls.spark.sql(
            "drop database if exists {} cascade".format(
                GeneralHandlerTest.TEST_DATABASE_NAME
            )
        ).collect()

    def tearDown(self):
        """Remove test databases and tables after every test."""
        self.spark.sql(
            "drop database if exists {} cascade".format(
                GeneralHandlerTest.TEST_DATABASE_NAME
            )
        ).collect()


if __name__ == "__main__":
    unittest.main()
