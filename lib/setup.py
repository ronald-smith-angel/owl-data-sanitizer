"""General project setup for domino-ingestion."""
import os
import re

from setuptools import setup, find_packages

SETUP_REQUIREMENTS = [
    "dataclasses==0.6",
    "pyspark==2.4.5",
]


def _get_version():
    """Read the __version__ value from src/domino_ingestion/version.py.

    We can't import the package because we're the installation script for the package,
    so we use regex and read the python file as a raw text file.
    """
    version_regex = re.compile(
        r"""^__version__\s=\s['"](?P<version>.*?)['"] """, re.MULTILINE | re.VERBOSE
    )
    version_file = os.path.join("src", "spark_validation", "version.py")
    with open(version_file) as handle:
        lines = handle.read()
        result = version_regex.search(lines)
        if result:
            return result.groupdict()["version"]
        raise ValueError("Unable to determine __version__")


setup(
    name="owl-sanitizer-data-quality",
    version=_get_version(),
    description="Data Quality for Pyspark jobs",
    author="Ronald Angel",
    author_email="ronaldsmithangel@gmail.com",
    url="https://github.com/ronald-smith-angel/owl-data-sanitizer.git",
    license="MIT",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=SETUP_REQUIREMENTS,
)
