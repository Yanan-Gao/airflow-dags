# Airflow

Apache Airflow is a flexible workflow management platform.

It is stewarded by [@dev-scrum-dataproc](https://thetradedesk.slack.com/archives/C04668SRF6J). You can reach out in [#scrum-dp-dataproc](https://app.slack.com/client/ES0HQDK1D/C04668SRF6J) or [#dev-airflow](https://app.slack.com/client/ES0HQDK1D/CK2QM4EHH).

You can access production Airflow here: [https://airflow2.gen.adsrvr.org/home](https://airflow2.gen.adsrvr.org/home).

**This README aims to give a brief overview. The full documentation is here:** [https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/](https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/)

[[_TOC_]]

# Developing

We recommend using PyCharm. 

Airflow 2 uses Python 3.12. You can use `pyenv` on Mac to install this version of Python.

Not all the dependencies are Windows-compatible. We recommend using a Mac for Airflow development. PyCharm Professional on Windows lets you use WSL to use a Python interpreter but your mileage may vary with this.

The `main-airflow-2` branch is for Airflow 2. `master` is used for Airflow 1.

# Running locally

Airflow can be run locally in a straightforward manner using Docker compose. This is most useful for checking your DAG file is valid, and ensuring it has the correct topology. You must be connected to the VPN of outside the office to get Airflow working.

You can start everything up by running
```bash
docker compose -f ./airflow-local/docker-compose.yml up -d --build --remove-orphans
```
You can access local instance at [http://localhost:3000/home](http://localhost:3000/home)

or 
```bash
bash ./airflow-local/start_airflow_local.sh
```

There is more comprehensive information here: [Running Airflow locally](https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/developing_and_testing/local_airflow/).

# Code Checks

Code checks are conducted as a part of CI steps.
You can run them locally using Docker:

### **Formatting**:

```bash
docker run --rm -v .:/tmp/code_checking --entrypoint "/bin/bash" --workdir "/tmp/code_checking" --pull always dev.docker.adsrvr.org/dataproc/ttd-airflow:prod-latest -c "yapf --in-place --recursive --parallel --style=code_tool_config/.style.yapf ."
```

Or you can run `Format code (yapf)` from PyCharm run configurations (top right corner of IDE)

### **Linting**:

```bash
docker run --rm -v .:/tmp/code_checking --entrypoint "/bin/bash" --workdir "/tmp/code_checking" --pull always dev.docker.adsrvr.org/dataproc/ttd-airflow:prod-latest -c "flake8 --append-config=code_tool_config/.flake8 ."
```
Or you can run `Lint code (flake8)` from PyCharm run configurations (top right corner of IDE)

### **Type Checking**:

```bash
docker run --rm -v .:/tmp/code_checking --entrypoint "/bin/bash" --workdir "/tmp/code_checking" --pull always dev.docker.adsrvr.org/dataproc/ttd-airflow:prod-latest -c "mypy --config-file=code_tool_config/.mypy.ini ."
```
Or you can run `Typecheck code (mypy)` from PyCharm run configurations (top right corner of IDE)

### All tools at once
Exec `run_tools.sh` from the root of the repo (you can do that through PyCharm as well: right click -> Run)
or
```bash
docker run --rm -v .:/tmp/code_checking \
 --entrypoint "/bin/bash" \
 --workdir "/tmp/code_checking" \
 --pull always \
 dev.docker.adsrvr.org/ttd-base/dataproc/ttd-airflow:prod-latest \
 -c "mypy --config-file=code_tool_config/.mypy.ini .; yapf --in-place --recursive --parallel --style=code_tool_config/.style.yapf .; flake8 --append-config=code_tool_config/.flake8 ."
```

### Through pre-commit hooks

Pre-commit hooks can be configured to locally automate checks.
For detailed instructions, see [Code-Checking Tools](https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/developing_and_testing/code_checking/)

# ProdTest Airflow

The prodTest environment allows you to create a self-contained instance of Airflow running your changeset, that also has access to external resources.
You can create such a prodTest from your MR. Once the required steps pass, you can press the `deploy_prodtest` button.

This will take a few moments. A comment on your MR will provide a link to your environment. 

There is more information around debugging and caveats here: [ProdTest Airflow](https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/developing_and_testing/prod_test/).

# Merging and deploying

DATAPROC have stewardship on changes in the `ttd` folder. You can add your own stewardship on your own files in [CODEOWNERS](CODEOWNERS). Otherwise, Airflow is self-service.

Merged changes are deployed straight away. Once the pipeline has run you should see your changes reflected in the UI.

# Debugging UI issues

You can use the following SumoLogic query to find webserver errors - this can be useful if you get errors such as `DAG seems to be missing` appearing.

```
(_sourceCategory="kubernetes/airflow2/prod")
AND _sourceName="web"
AND _loglevel="ERROR"
```

There's more details here: [Sumo Logging](https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/developing_and_testing/sumo_logging/).

# Migrating to Airflow 2 from Airflow 1.

There is information about the migration here: [Airflow 2](https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/airflow2_migration/introduction/).

There is a tool in this repo to allow you to preserve git history when moving files from `master` to `main-airflow-2`:

1. Checkout a new branch from `main-airflow-2`
2. Run the tool with a comma-separated list of the file locations in `master`: `./local_tools/preserve_git_history/move_airflow1_files.sh dags/dataproc/task_service/my_task.py`
3. Use `git mv` to move the files to their new location: `git mv dags/dataproc/task_service/my_task.py src/dags/dataproc/task_service/my_task.py`
