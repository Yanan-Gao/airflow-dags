# Preserve git history between Airflow branches

## 0. Install necessary packages

This script requires `git-filter-repo` package to be installed - please find the installation guide [here](https://github.com/newren/git-filter-repo/blob/main/INSTALL.md)

## 1. Run the script

**It's best to start with a clean, fresh branch.** In particular, unstaged commits can cause unsavoury behaviour and so the tool will fail if all changes are not committed.

Run the script using a comma-separated list of files, **using the locations of the files as the exist in the master branch.**

For instance, if we wanted to move the DATAPROC TaskService DAG files, which are located in `master` at 
- `dags/dataproc/task_service/provisioning_to_parquet_change_tracking_task.py`
- `dags/dataproc/task_service/task_service_tasks.py`

We would run the tool as follows:
```bash
./local_tools/preserve_git_history/move_airflow1_files.sh dags/dataproc/task_service/task_service_tasks.py,dags/dataproc/task_service/provisioning_to_parquet_change_tracking_task.py
```

The script clones a temporary copy of the repo, gets the git history of the files, and then applies it to your Airflow2 branch.

## 2. Move the files

`main-aiflow-2` uses a different file structure to `master`. Your moved files will be at their old locations and will need to be moved.

You can use `git mv` to achieve this. The syntax is `git mv old/path.py new/path.py`. Continuing with the previous example, you would run:

```bash
git mv dags/dataproc/task_service/provisioning_to_parquet_change_tracking_task.py src/dags/dataproc/task_service/provisioning_to_parquet_change_tracking_task.py
git mv dags/dataproc/task_service/task_service_tasks.py src/dags/dataproc/task_service/task_service_tasks.py
```

## 3. Reach out to DATAPROC upon pushing and merging

Ordinarily, you cannot push commits unless they meet this requirement:
> Users can only push commits to this repository if the committer email is one of their own verified emails.

If you reach out to the DATAPROC team in [#scrum-dp-dataproc](https://app.slack.com/client/ES0HQDK1D/C04668SRF6J), we can disable
this requirement to allow you to push your changes.

**The git history won't be preserved correctly if you merge in without reaching out to DATAPROC for assistance.**

Commits are squashed automatically upon merge to the Airflow repo. This will destroy the git history, and must be temporarily 
disabled in order for the above to have any effect. Please reach out to the DATAPROC team for assistance.
