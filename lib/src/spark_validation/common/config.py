"""Module representing config data."""
import json
from abc import ABC


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

    @staticmethod
    def _create_config(config):
        correctness_validations = {
            rule["column"]: rule["rule"] for rule in config["correctness_validations"]
        }
        parent_children_validations = [
            (rule["column"], rule["parent"])
            for rule in config["parent_children_constraints"]
        ]

        completeness_validations = {
            rule["column"]: rule["rule"] for rule in config["completeness_validations"]
        }
        return Config(
            source_df=config["source_table"]["name"],
            id_col_name=config["source_table"]["id_column"],
            correctness_rules_dict=correctness_validations,
            parent_children_validation_pairs=parent_children_validations,
            completeness_rules_dic=completeness_validations,
            comparable_dfs_list=config["compare_related_tables_list"],
            output_correctness_table=config["source_table"]["output_correctness_table"],
            output_completeness_table=config["source_table"][
                "output_completeness_table"
            ],
            output_comparison_table=config["source_table"]["output_comparison_table"],
        )

    @staticmethod
    def parse(file):
        """parse a json file to a config object."""
        config = json.load(file)
        return Config._create_config(config)

    @staticmethod
    def parse_text(str_file):
        """parse a json file to a config object."""
        config = json.loads(str_file)
        return Config._create_config(config)
