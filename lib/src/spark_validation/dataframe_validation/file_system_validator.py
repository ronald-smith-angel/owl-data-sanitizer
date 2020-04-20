import argparse
import logging
import os

from pyspark.sql import SparkSession

from spark_validation.common.config import Config
from spark_validation.common.constants import Constants
from spark_validation.dataframe_validation.dataframe_validator import DataframeValidator

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))


class CreateFSValidationDF:
    """Class to create validations tables."""

    logger = logging.getLogger(__name__)

    @staticmethod
    def validate(ss, config):
        """Apply validation process using config input file."""
        source_read_df = ss.read.format("csv").option("header", "true").load(config.source_df)
        comparable_dfs_list = [(t, ss.read.format("csv").option("header", "true").load(t)) for t in
                               config.comparable_dfs_list]

        validator = DataframeValidator(
            spark=ss,
            source_df=source_read_df,
            id_col_name=config.id_col_name,
            correctness_rules_dict=config.correctness_rules_dict,
            parent_children_validation_pairs=config.parent_children_validation_pairs,
            completeness_rules_dic=config.completeness_rules_dic,
            comparable_dfs_list=comparable_dfs_list,
        )

        processed_df = validator.process()
        completeness_df = processed_df.limit(1).select(
            Constants.OVER_ALL_COUNT_COL,
            Constants.IS_ERROR_COL + Constants.OVER_ALL_COUNT_COL,
            Constants.DATE_TIME_REPORT_COL,
        )

        correctness_df = processed_df.drop(
            Constants.OVER_ALL_COUNT_COL,
            Constants.IS_ERROR_COL + Constants.OVER_ALL_COUNT_COL,
        )
        comparison_df = validator.compare()

        correctness_df.coalesce(1).write.mode("overwrite").json(config.output_correctness_table)
        completeness_df.coalesce(1).write.mode("overwrite").json(config.output_completeness_table)
        comparison_df.coalesce(1).write.mode("overwrite").json(config.output_comparison_table)


def main(args):
    """Run the main create table function using the sys arguments."""
    spark_session = SparkSession.builder.getOrCreate()
    spark_session.conf.set("spark.sql.debug.maxToStringFields", "1000")
    spark_session.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    with open(args.config) as f:
        config = Config.parse(f)

    CreateFSValidationDF.validate(spark_session, config)


def create_parser():
    """Parse sys arguments and return parser object."""
    parser = argparse.ArgumentParser(description="Hive Validation")
    parser.add_argument(
        "-c", dest="config", action="store", help="config file", required=True,
    )
    return parser


def init():
    """Wrap to make main call function testable by sending parsed arguments."""
    parser = create_parser()
    args = parser.parse_args()
    main(args)


if __name__ == "__main__":
    init()
