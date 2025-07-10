#!/bin/bash

# Check if no arguments are passed
if [ $# -eq 0 ]; then
    echo "Please provide a comma separated list of files:
     eg: ./move_airflow1_files.sh dags/dataproc/task_service/task_service_tasks.py,dags/dataproc/task_service/provisioning_to_parquet_change_tracking_task.py"
    exit 1
fi

# Check there are no unstaged files
git diff --cached --exit-code > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Please commit any unstaged changes before running this script!"
    exit 1
fi


echo "Will move the following files: "
IFS=',' read -r -a files <<< "$1"
for file in "${files[@]}"; do
    echo "$file"
done

# Clone a temporary copy of the repo
rm -rf /tmp/airflow1-temp
git clone git@gitlab.adsrvr.org:thetradedesk/teams/dataproc/airflow-dags.git /tmp/airflow1-temp -b master

historical_file_names=$(printf "%s\n" "${files[@]}" | xargs -I {} git --git-dir=/tmp/airflow1-temp/.git --work-tree=/tmp/airflow1-temp blame --show-name {} | cut -f 2 -d' ' | sort | uniq )

command="git --git-dir=/tmp/airflow1-temp/.git --work-tree=/tmp/airflow1-temp filter-repo --force $(printf " --path %s" $historical_file_names)"

echo "$command"

git --git-dir=/tmp/airflow1-temp/.git --work-tree=/tmp/airflow1-temp remote rm origin
eval "$command"

git remote add local-airflow1-temp /tmp/airflow1-temp/

git pull local-airflow1-temp master --allow-unrelated-histories --no-rebase

git remote remove local-airflow1-temp
