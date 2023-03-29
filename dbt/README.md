# dbt Core

## Setup Connection (for dbt Core)

> Info: Make sure, that you have cd'ed into the `dbt` directory. Run `cd dbt` otherwise:

Run the following command to setup your BigQuery connection:
```bash
pipenv run dbt init
```

An interactive setup dialog will appear:
* *Which database would you like to use?*  
  Type `1` for BigQuery.
* *Desired authentication method option:*  
  Type `1` for OAuth (recommended for local development, not for production)
* *Project (GCP project id):*  
  Enter your project id, e.g. `white-goose-338318`.
* *Dataset (the name of your dbt dataset):*  
  Enter the name of the dataset in BigQuery, e.g. `e_commerce`.
* *threads (1 or more):*  
  Type `1`.
* *job_execution_timeout_seconds [300]:*  
  Type `300`.
* *Desired location option (enter a number):*  
  Type `1` for US (even if this is not your desired location). 
  
Because the dbt setup dialog only offers a selection for the multi-regions `US` and `EU` and not for single regions, you need to set a single region manually. The location of BigQuery needs to match the location of your GCS bucket. If you bucket is in a dual region, choose one of the locations in the region for your BigQuery dataset. Otherwise you can not use external tables, but they are needed for this project.

Run this command to edit the location manually:
```bash
nano ~/.dbt/profiles.yml
```
1. Move the cursor with the arrow keys to the line with the label `location`. Delete the `US` and type the name of your desired location e.g. `us-west1`. 
2. Press `ctrl + o` and press `enter` to save the changes.
3. Press `ctrl + c` to close the editor.


To connect to BigQuery using the OAuth method, follow these steps:

1. Make sure the gcloud command is [installed on your computer](https://cloud.google.com/sdk/docs/install)
2. Activate the application-default account with

```bash
gcloud auth application-default login
```

A browser window will open or you will ber equested to open a link in your browser. Follow the instructions to authenticate the CLI.

Consult the [dbt documentation](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#authentication-methods) for instructions for other authentication methods.

Test your connection with this command:
```bash
pipenv run dbt debug
```
Everything is set up correctly, if no error message shows up.


## Run dbt

Install dbt packages:
```bash
pipenv run dbt deps
```

Create external tables in DWH:
```bash
pipenv run dbt run-operation stage_external_sources
```

Run transformations and tests:
```bash
pipenv run dbt build
```

Generate documentation:
```bash
pipenv run dbt docs generate --no-compile
pipenv run dbt docs serve --port 8000
```

Open the link http://localhost:8000 in your browser.