Feature: showing popmon reports pyspark
  Scenario: test kedro-popmon reading CSV with PySpark
    Given we have created new project using kedro new with pyspark
    And we prepare pyspark-iris project
    And we have installed project dependencies with kedro install
    And we prepare CSV SparkDataSet
    And we have installed kedro-popmon plugin
    And we have initialized kedro-popmon plugin
    And we add a date column to our dataset
    And we configured popmon.yml file
    And we configured parameters file
    When we run the project
    Then popmon report is generated

  Scenario: test kedro-popmon reading Parquet with PySpark
    Given we have created new project using kedro new with pyspark
    And we prepare pyspark-iris project
    And we have installed project dependencies with kedro install
    And we prepare Parquet SparkDataSet
    And we have installed kedro-popmon plugin
    And we have initialized kedro-popmon plugin
    And we configured popmon.yml file
    And we configured parameters file
    When we run the project
    Then popmon report is generated