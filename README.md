# Owl Data Sanitizer: A light Spark data validation framework

[![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://github.com/ronald-smith-angel/owl-data-sanitizer/blob/develop/license.md)

This is a small framework for data quality validation. This first version works reading spark dataframes from local 
datasources like local system, s3 or hive and delivers hive tables with quality reports.

Let's follow this example:

Input data from a hive table:

```
+----------+--------------+--------+---------+------------------+---------+
|GENERAL_ID|          NAME|    CODE|ADDR_DESC|ULTIMATE_PARENT_ID|PARENT_ID|
+----------+--------------+--------+---------+------------------+---------+
|         1|Dummy 1 Entity|12000123|     null|              null|     null|
|         2|          null|    null|     null|                 2|        2|
|         3|          null|12000123|     null|                 3|        3|
|         4|             1|       1|     null|                 4|        4|
|         5|             1|12000123|     null|                 5|        5|
|         6|          null|       3|     null|                 6|        6|
|      null|          null|12000123|     null|                11|        7|
|         7|             2|    null|     null|                 8|        8|
+----------+--------------+--------+---------+------------------+---------+
```

following this validation config with 4 sections:

1. `source_table` including the table metadata.
2. `correctness_validations` including correctness validations per column. 
the rule must be a valid spark SQL expression.
3. `parent_children_constraints` including children parent constrains. 
This means that any parent id should be valid id.
4. `compare_related_tables_list` including comparison with other tables or 
the same table in other environments.

```
{
  "source_table": {
    "name": "test.data_test",
    "id_column": "GENERAL_ID",
    "output_correctness_table": "test.data_test_correctness",
    "output_completeness_table": "test.data_test_completeness",
    "output_comparison_table": "test.data_test_comparison"
  },
  "correctness_validations": [
    {
      "column": "CODE",
      "rule": "CODE is not null and CODE != '' and CODE != 'null'"
    },
    {
      "column": "NAME",
      "rule": "NAME is not null and NAME != '' and NAME != 'null'"
    },
    {
      "column": "GENERAL_ID",
      "rule": "GENERAL_ID is not null and GENERAL_ID != '' and GENERAL_ID != 'null' and CHAR_LENGTH(GENERAL_ID) < 4"
    }
  ],
  "completeness_validations": [
    {
      "column": "OVER_ALL_COUNT",
      "rule": "OVER_ALL_COUNT <= 7"
    }
  ],
  "parent_children_constraints": [
    {
      "column": "GENERAL_ID",
      "parent": "ULTIMATE_PARENT_ID"
    },
    {
      "column": "GENERAL_ID",
      "parent": "PARENT_ID"
    }
  ],
  "compare_related_tables_list": ["test.diff_df", "test.diff_df_2"]
}
```

Therefore, these results are delivered in two output hive tables:

a). Correctness Report.

- You will see and output col per validation col showing either 1 when there is error or 0 when is clean.
- Sum of error per columns.

```
+----------+-------------+-------------+-------------------+--------------------------------------+-----------------------------+-------------+--------------------------+-----------------+-----------------+-----------------------+------------------------------------------+---------------------------------+-----------------+
|GENERAL_ID|IS_ERROR_CODE|IS_ERROR_NAME|IS_ERROR_GENERAL_ID|IS_ERROR_GENERAL_ID_ULTIMATE_PARENT_ID|IS_ERROR_GENERAL_ID_PARENT_ID|IS_ERROR__ROW|dt                        |IS_ERROR_CODE_SUM|IS_ERROR_NAME_SUM|IS_ERROR_GENERAL_ID_SUM|IS_ERROR_GENERAL_ID_ULTIMATE_PARENT_ID_SUM|IS_ERROR_GENERAL_ID_PARENT_ID_SUM|IS_ERROR__ROW_SUM|
+----------+-------------+-------------+-------------------+--------------------------------------+-----------------------------+-------------+--------------------------+-----------------+-----------------+-----------------------+------------------------------------------+---------------------------------+-----------------+
|null      |0            |1            |1                  |1                                     |0                            |1            |2020-04-17 09:39:04.783505|2                |4                |1                      |2                                         |1                                |5                |
|3         |0            |1            |0                  |0                                     |0                            |1            |2020-04-17 09:39:04.783505|2                |4                |1                      |2                                         |1                                |5                |
|7         |1            |0            |0                  |1                                     |1                            |1            |2020-04-17 09:39:04.783505|2                |4                |1                      |2                                         |1                                |5                |
|5         |0            |0            |0                  |0                                     |0                            |0            |2020-04-17 09:39:04.783505|2                |4                |1                      |2                                         |1                                |5                |
|6         |0            |1            |0                  |0                                     |0                            |1            |2020-04-17 09:39:04.783505|2                |4                |1                      |2                                         |1                                |5                |
|4         |0            |0            |0                  |0                                     |0                            |0            |2020-04-17 09:39:04.783505|2                |4                |1                      |2                                         |1                                |5                |
|2         |1            |1            |0                  |0                                     |0                            |1            |2020-04-17 09:39:04.783505|2                |4                |1                      |2                                         |1                                |5                |
|1         |0            |0            |0                  |0                                     |0                            |0            |2020-04-17 09:39:04.783505|2                |4                |1                      |2                                         |1                                |5                |
+----------+-------------+-------------+-------------------+--------------------------------------+-----------------------------+-------------+--------------------------+-----------------+-----------------+-----------------------+------------------------------------------+---------------------------------+-----------------+
```
b) Completeness Report.
- The overall count of the dataframe.
- Column checking if the overall count is complete, example: `IS_ERROR_OVER_ALL_COUNT`.
```
+--------------+-----------------------+--------------------------+
|OVER_ALL_COUNT|IS_ERROR_OVER_ALL_COUNT|dt                        |
+--------------+-----------------------+--------------------------+
|8             |1                      |2020-04-17 09:39:04.783505|
+--------------+-----------------------+--------------------------+
```

c). Comparison of schema and values with related dataframes. 

NOTE: the result includes for now only the ids that are different and a further 
join with the source data to see differences is needed.

```
+--------------+----------------------------------+-----------------+------------------+-----------------+--------------------------+
|df            |missing_cols_right                |missing_cols_left|missing_vals_right|missing_vals_left|dt                        |
+--------------+----------------------------------+-----------------+------------------+-----------------+--------------------------+
|test.diff_df_2|GENERAL_ID:string,ADDR_DESC:string|GENERAL_ID:int   |                  |                 |2020-04-17 09:39:07.572483|
|test.diff_df  |                                  |                 |6,7               |                 |2020-04-17 09:39:07.572483|
+--------------+----------------------------------+-----------------+------------------+-----------------+--------------------------+
```

#Installation

Install owl sanitizer from PyPI:

```pip install owl-sanitizer-data-quality```

Then you can call the library.

```
from spark_validation.dataframe_validation.dataframe_validator import CreateHiveValidationDF
from spark_validation.common.config import Config

spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
with open(PATH_TO_CONIGIG_FILE) as f:
        config = Config.parse(f)
CreateHiveValidationDF.validate(spark_session, config)
```