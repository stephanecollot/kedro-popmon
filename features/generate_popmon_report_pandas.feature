Feature: showing popmon reports pandas
    Scenario: test kedro-popmon using Pandas dataframe
     Given we have created new project using kedro new with pandas
     And we have installed project dependencies with kedro install
     And we have installed kedro-popmon plugin
     And we have initialized kedro-popmon plugin
     And we prepare Pandas CSVDataset
     And we add a date column to our dataset
     And we add date column to de pipeline
     And we configured popmon.yml file
     And we configured parameters file
     When we run the project
     Then popmon report is generated