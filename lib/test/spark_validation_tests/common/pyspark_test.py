"""Module with general pyspark function to tests."""

import logging
import os
import shutil
import unittest

from pyspark.sql import SparkSession


class PySparkTest(unittest.TestCase):
    """Class with general pyspark functions to tests."""

    derby_home = "/tmp/derby_home"
    spark_warehouse = "/tmp/spark_warehouse"

    @classmethod
    def suppress_py4j_logging(cls):
        """Set waring level for py4j logs."""
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)

    @classmethod
    def create_session(cls):
        """Create dummy spark config."""
        derby_property = "-Dderby.system.home={}".format(cls.derby_home)
        return (
            SparkSession.builder.master("local[*]")
            .appName("local-testing-pyspark-context")
            .config("spark.driver.extraJavaOptions", derby_property)
            .config("spark.sql.warehouse.dir", cls.spark_warehouse)
            .enableHiveSupport()
            .getOrCreate()
        )

    @classmethod
    def setUpClass(cls):
        """Init needed directories and sessions."""
        cls.suppress_py4j_logging()

        if os.path.exists(cls.derby_home):
            shutil.rmtree(cls.derby_home)
        if os.path.exists(cls.spark_warehouse):
            shutil.rmtree(cls.spark_warehouse)
        os.mkdir(cls.derby_home)
        os.mkdir(cls.spark_warehouse)
        cls.spark = cls.create_session()

    @classmethod
    def tearDownClass(cls):
        """Remove directories and sessions."""
        cls.spark.stop()
        shutil.rmtree(cls.derby_home)
        shutil.rmtree(cls.spark_warehouse)
