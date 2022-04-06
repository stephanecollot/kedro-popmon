import os
import subprocess
from pathlib import Path

import click
import semver
import yaml
from click import secho
import logging

SPARK2_JAR = "org.diana-hep:histogrammar-sparksql_2.11:1.0.4"
SPARK3_JAR = "io.github.histogrammar:histogrammar_2.12:1.0.20,io.github.histogrammar:histogrammar-sparksql_2.12:1.0.20"
logger = logging.getLogger("KedroPopmon")


@click.group(name="Popmon")
def commands():
    """Kedro plugin for interactions with popmon.
    """
    pass  # pragma: no cover


@commands.group()
def popmon():
    """Run Kedro popmon commands"""


@commands.command()
def init():
    """
    Create a new popmon config file
    """
    project_path = Path.cwd()
    files = os.listdir(f"{project_path}/conf/base") + os.listdir(
        f"{project_path}/conf/local"
    )
    popmon_config_present = False
    for file in files:
        if "popmon" in file:
            popmon_config_present = True
            break
    if not popmon_config_present:
        with open(f"{project_path}/conf/base/popmon.yml", "w") as f:
            f.write("# Please enter the structure of popmon reporting\n")
            f.write(
                "# datasource: # must be one of datasources avaliable in catalog.yml, must be provided\n"
            )
            f.write(
                "# date_column: # the column you wish to refer as the data column, must be provided\n"
            )
            f.write(
                "# date_column_format: # the date format if not present equals to '%Y-%m-%d %H:%M:%S'\n"
            )
            f.write("# columns: # list of columns, by default the list is empty\n")
            f.write("# - value1 # column names you wish to analyze\n")
            f.write("# - value2\n")
        os.system("mkdir -p data/08_reporting/popmon")
    setup_pyspark_jars(project_path)
    exit(0)


popmon.add_command(init)


def get_spark_major_version():
    secho("Checking Apache Spark version, it may take a few secs...", fg="green")
    p = subprocess.Popen(
        "spark-shell --version 2> version && cat version && rm version",
        shell=True,
        stdout=subprocess.PIPE,
    )
    try:
        version = p.communicate()[0].decode("utf-8")
        start = version.index("version") + len("version")
        end = version.index("\n", start)
        version = version[start:end].strip()
        parsed = semver.parse(version)['major']
    except ValueError:
        parsed = -1
    return parsed


def setup_pyspark_jars(project_path):
    file = f"{project_path}/conf/base/spark.yml"
    conf = {}
    if os.path.exists(file):
        with open(file, "r") as yaml_file:
            conf = yaml.safe_load(yaml_file)

    with open(file, "w") as yaml_file:
        existing_jars = None
        if "spark.jars.packages" in conf.keys():
            existing_jars = set(conf.get("spark.jars.packages", "").split(","))
        else:
            existing_jars = set()
        spark_major_version = get_spark_major_version()
        if spark_major_version == 2:
            spark_jar = SPARK2_JAR
        elif spark_major_version == 3:
            spark_jar = SPARK3_JAR
        elif spark_major_version == -1:
            logger.error("Apache Spark version has not been properly detected.")
        else:
            logger.error("Detected unsupported "
                         f"Apache Spark version {spark_major_version}")
        if spark_major_version in (2, 3):
            for j in spark_jar.split(','):
                if j not in existing_jars:
                    existing_jars.add(j)
            conf.update({"spark.jars.packages": ",".join(existing_jars)})
            yaml.safe_dump(conf, yaml_file)
