import os
import sys
import time
from airflow.models import DagBag

from ttd.eng_org_structure_utils import warm_scrum_teams_cache
from ttd.secrets.vault_secrets_backend import DagLoadSecretsChecker
from airflow.secrets import metastore

metastore.MetastoreBackend.get_variable = lambda *args, **kwargs: None
timings = {}


class FakeDagBag(DagBag):

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        DagLoadSecretsChecker.set_context(filepath)
        start = time.time()
        print(f"Starting import of {filepath}")
        found_dags = super().process_file(filepath, only_if_updated, safe_mode)
        elapsed = time.time() - start
        print(f"Finished import of {filepath} in {elapsed:.2f}s")
        timings[filepath] = elapsed
        return found_dags


def print_import_timings():
    print("\n\nImport timings (top 10 slowest):")
    for filepath, elapsed in sorted(timings.items(), key=lambda kv: kv[1], reverse=True)[:10]:
        print(f"{elapsed:.2f}s\t{filepath}")


warm_scrum_teams_cache()

dag_bag = FakeDagBag(dag_folder=os.getenv("AIRFLOW__CORE__DAGS_FOLDER"))
valid_dags = dict(sorted(dag_bag.dags.items()))

print("\n\nValid DAGs:")
for dag in valid_dags.values():
    print(f"{dag.dag_id} - {dag.relative_fileloc}")

if len(dag_bag.import_errors) > 0:
    print("\n\nERRORS!: \n")
    for file, traceback in dag_bag.import_errors.items():
        print(file)
        print(traceback)

    for file, exception in DagLoadSecretsChecker.VIOLATIONS.items():
        print(file)
        print("Attempted to access: ", exception.variable)
        print(exception.exception)

    print_import_timings()
    sys.exit(1)
else:
    if len(DagLoadSecretsChecker.VIOLATIONS) > 0:
        print("\n\nERRORS! Dags access variables or connections outside of execution context: \n")
        for file, exception in DagLoadSecretsChecker.VIOLATIONS.items():
            print(file)
            print("Attempted to access: ", exception.variable)
            print(exception.exception)
        print_import_timings()
        sys.exit(1)
    print_import_timings()
    sys.exit(0)
