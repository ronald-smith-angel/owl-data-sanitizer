"""Module representing config data."""
import json
import sys
import yaml
from abc import ABC

from pyspark.sql.utils import AnalysisException


class Config(ABC):
    """Class with config data."""

    def __init__(
        self,
        source_df,
        id_col_name,
        correctness_rules_dict,
        parent_children_validation_pairs,
        completeness_rules_dic,
        comparable_dfs_list,
        output_correctness_table,
        output_completeness_table,
        output_comparison_table,
        unique_column_group_values_per_table=[],
        fuzzy_deduplication_distance=0,
    ):
        self.source_df = source_df
        self.id_col_name = id_col_name
        self.correctness_rules_dict = correctness_rules_dict
        self.parent_children_validation_pairs = parent_children_validation_pairs
        self.completeness_rules_dic = completeness_rules_dic
        self.comparable_dfs_list = comparable_dfs_list
        self.output_correctness_table = output_correctness_table
        self.output_completeness_table = output_completeness_table
        self.output_comparison_table = output_comparison_table
        self.unique_column_group_values_per_table = unique_column_group_values_per_table
        self.fuzzy_deduplication_distance = fuzzy_deduplication_distance

    @staticmethod
    def _create_config(config):
        try:
            correctness_validations = {
                rule["column"]: rule["rule"]
                for rule in config["correctness_validations"]
            }
            parent_children_validations = [
                (rule["column"], rule["parent"])
                for rule in config["parent_children_constraints"]
            ]

            completeness_overall_rule = config["completeness_validations"]["overall"]
            completeness_validations = {
                completeness_overall_rule["column"]: completeness_overall_rule["rule"]
            }
            return Config(
                source_df=config["source_table"]["name"],
                id_col_name=config["source_table"]["id_column"],
                correctness_rules_dict=correctness_validations,
                parent_children_validation_pairs=parent_children_validations,
                completeness_rules_dic=completeness_validations,
                comparable_dfs_list=config["compare_related_tables_list"],
                output_correctness_table=config["source_table"][
                    "output_correctness_table"
                ],
                output_completeness_table=config["source_table"][
                    "output_completeness_table"
                ],
                output_comparison_table=config["source_table"][
                    "output_comparison_table"
                ],
                unique_column_group_values_per_table=config["source_table"][
                    "unique_column_group_values_per_table"
                ]
                if (
                    "unique_column_group_values_per_table"
                    in config["source_table"].keys()
                )
                else [],
                fuzzy_deduplication_distance=config["source_table"][
                    "fuzzy_deduplication_distance"
                ]
                if ("fuzzy_deduplication_distance" in config["source_table"].keys())
                else 0,
            )
        except KeyError as e:
            print(
                "The config file has key error, check source_table, correctness_validations,"
                " completeness_validations, parent_children_constraints, compare_related_tables_list as mandatory)"
                ' - reason "%s"' % str(e)
            )

    @staticmethod
    def parse(file):
        """parse a json file to a config object."""
        try:
            config = json.load(file) if 'json' in file.name else yaml.load(file)
        except OSError:
            print("Could not open/read file:", file)
            sys.exit()
        return Config._create_config(config)

    @staticmethod
    def parse_text(str_file):
        """parse a json file to a config object."""
        try:
            config = json.loads(str_file)
        except AnalysisException:
            print("Could not open/read file:", str_file)
            sys.exit()
        return Config._create_config(config)
