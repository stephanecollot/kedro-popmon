# Kedro Popmon

Kedro-popmon is a plugin to integrate popmon reporting with kedro.  
This plugin allows you to automate the process of popmon feature and output stability monitoring.   

## How to install
```bash
pip install kedro-popmon
```

## How to use it

Run `kedro popmon init` in a existing kedro project.  
You will get the `conf/base/popmon.yml`.  
Edit `conf/base/popmon.yml` according to the way described in it.  
Example:
```
example_iris_data:
  date_column: date
  columns:
    - sepal_length
```

When you do `kedro run` you shall expect your reports to be generated in 
`data/08_reporting/` folder.