import glob
import shutil

import yaml
from behave import given, when, then
import subprocess
import os
import pandas as pd
import numpy as np

OK_EXIT_CODE = 0
STARTERS_REPO_URL = os.getenv('STARTERS_REPO_URL',
                              "https://github.com/quantumblacklabs/kedro-starters")
iris_ds_name = 'example_iris_data'


def run_sample_project(context, starter):
    res = subprocess.run(
        [context.mkdir, "code"],
        cwd=f"{context.temp_dir}"
    )
    assert res.returncode == OK_EXIT_CODE
    res = subprocess.run(
        [context.kedro, "new", "--config",
         f"{context.root_project_dir}/features/steps/config.yaml",
         "--starter",
         STARTERS_REPO_URL,
         "--checkout", "0.17.3", "--directory", starter],
        cwd=f"{context.temp_dir}"
    )
    assert res.returncode == OK_EXIT_CODE


@given("we have created new project using kedro new with pandas")
def run_sample_project_pandas(context):
    run_sample_project(context, "pandas-iris")


@given("we have created new project using kedro new with pyspark")
def run_sample_project_pyspark(context):
    run_sample_project(context, "pyspark-iris")


@given("we have installed project dependencies with kedro install")
def install_project(context):
    path = f"code/{context.repo_name}/"
    res = subprocess.run([context.kedro, "install"], cwd=f"{context.temp_dir}/{path}")
    assert res.returncode == OK_EXIT_CODE


@given("we have installed kedro-popmon plugin")
def install_kedro_popmon(context):
    res_base = subprocess.run(
        [context.pip, "install", "-e", "."], cwd=context.root_project_dir
    )
    res_extra = subprocess.run(
        [context.pip, "install", "-e", ".[spark]"], cwd=context.root_project_dir
    )
    assert res_base.returncode == OK_EXIT_CODE
    assert res_extra.returncode == OK_EXIT_CODE


@given("we have initialized kedro-popmon plugin")
def initialized_kedro_popmon(context):
    path = f"code/{context.repo_name}/"
    res = subprocess.run(
        [context.kedro, "popmon", "init"], cwd=f"{context.temp_dir}/{path}"
    )
    assert res.returncode == OK_EXIT_CODE


@given("we add a date column to our dataset")
def add_columns_to_dataset(context):
    path = f"{context.temp_dir}/code/{context.repo_name}/data/01_raw/iris.csv"
    df = pd.read_csv(path)
    df["date"] = [
        np.random.choice([f"2020-01-{i}" for i in range(10, 20)])
        for _ in range(df.shape[0])
    ]
    df.to_csv(path, index=False)


@given("we add date column to de pipeline")
def add_date_column_to_de_pipeline(context):
    file = f"{context.temp_dir}/code/{context.repo_name}/src/" \
           f"{context.package_name}/pipelines/data_engineering/nodes.py"
    with open(file) as f:
        lines = f.readlines()
    new_lines = []
    for line in lines:
        new_lines.append(line)
        if '"target",' in line:
            new_lines.append('        "date"')
    with open(file, "w") as f:
        for line in new_lines:
            f.write(line)


@given("we configured popmon.yml file")
def set_popmon_conf(context):
    file = f"{context.temp_dir}/code/{context.repo_name}/conf/base/popmon.yml"
    conf = {
        f'{iris_ds_name}': {
            'date_column': 'date',
            'columns': ['sepal_length']
        }
    }

    with open(file, 'w') as yaml_file:
        yaml.safe_dump(conf, yaml_file)


@given("we configured parameters file")
def set_parameters(context):
    file = f"{context.temp_dir}/code/{context.repo_name}/conf/base/parameters.yml"
    conf = {}
    conf.update(
        {
            'example_test_data_ratio': 0.2,
            'example_num_trees': 10,
            'example_num_train_iter': 10000,
            'example_learning_rate': 0.01
        }
    )
    with open(file, 'w') as yaml_file:
        yaml.safe_dump(conf, yaml_file)


def set_catalog_input_ds(context, conf):
    file = f"{context.temp_dir}/code/{context.repo_name}/conf/base/catalog.yml"
    conf.update({'example_classifier': {
        'type': 'MemoryDataSet',
        'copy_mode': 'assign'
    }
    })
    with open(file, 'w') as yaml_file:
        yaml.safe_dump(conf, yaml_file)


def set_spark_input_parquet(context):
    conf = {
        f'{iris_ds_name}': {
            'type': 'spark.SparkDataSet',
            'filepath': f'data/01_raw/iris.parquet'
        }
    }
    set_catalog_input_ds(context, conf)


def set_spark_input_csv(context):
    conf = {
        f'{iris_ds_name}': {
            'type': 'spark.SparkDataSet',
            'filepath': 'data/01_raw/iris.csv',
            'file_format': 'csv',
            'load_args': {
                'header': True,
                'inferSchema': True
            },
            'save_args': {
                'sep': ',',
                'header': True,
            }
        }
    }
    set_catalog_input_ds(context, conf)


def set_pandas_input_csv(context):
    conf = {f'{iris_ds_name}': {
        'type': 'pandas.CSVDataSet',
        'filepath': 'data/01_raw/iris.csv'
    }
    }
    set_catalog_input_ds(context, conf)


def copy_ds(context, ds_name):
    dst = f"{context.temp_dir}/code/{context.repo_name}/data/01_raw/{ds_name}"
    src = f"{context.root_project_dir}/resources/data/{ds_name}"
    if os.path.isdir(src):
        shutil.copytree(src, dst)
    else:
        shutil.copy(src, dst)


@given("we prepare Pandas CSVDataset")
def prepare_pandas_ds_csv(context):
    ds_name = "iris.csv"
    copy_ds(context, ds_name)
    set_pandas_input_csv(context)


@given("we prepare CSV SparkDataset")
def prepare_spark_ds_csv(context):
    ds_name = "iris.csv"
    copy_ds(context, ds_name)
    set_spark_input_csv(context)


@given("we prepare Parquet SparkDataSet")
def prepare_spark_ds_parquet(context):
    ds_name = "iris.parquet"
    copy_ds(context, ds_name)
    set_spark_input_parquet(context)


@given("we prepare pyspark-iris project")
def prepare_pypspark_project(context):
    path = f"{context.temp_dir}/code/{context.repo_name}/src/requirements.txt"
    with open(path, 'a+') as file:
        file.write(f'pyspark=={os.getenv("SPARK_VERSION", "2.4.0")}')


@when("we run the project")
def run_project(context):
    path = f"{context.temp_dir}/code/{context.repo_name}/"
    popmon_env = os.environ.copy()
    """
    Example of how you can to set pyspark submit parameters:
    popmon_env['PYSPARK_SUBMIT_ARGS'] = \
        '--repositories https://artifactory.internal/artifactory/MAVEN ' \
        'pyspark-shell '
    """

    res = subprocess.run([context.kedro, "run"], cwd=path, env=popmon_env)
    assert res.returncode == OK_EXIT_CODE, f"{res}"


@then("popmon report is generated")
def check_report_is_generated(context):
    path = f"{context.temp_dir}/code/{context.repo_name}/data/08_reporting/popmon/"
    assert "html" in set([x[-4:] for x in os.listdir(path)]), os.listdir(path)
