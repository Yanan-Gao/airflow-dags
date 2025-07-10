from airflow.models import BaseOperator
from airflow.utils.context import Context


class NamespacedMixin(BaseOperator):

    def __init__(self, job_name: str, task_id: str, developer_mode: bool = True, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.job_name = job_name
        self.developer_mode = developer_mode

    def get_namespace(self, context: Context):
        if self.developer_mode:
            return "spark"
        else:
            team = context["task"].owner.lower()
            return f"spark-{team}"

    def get_labels(self, context: Context):
        return {"airflow/dag-task-id": self.job_name, "airflow/dag-logical-date": context["logical_date"].format("YYYY-MM-DDTHH.mm.ss")}

    def _fetch_or_create(self, list_func, create_func, get_func, namespace, body, labels: dict[str, str] | None = None):
        label_str = ",".join([f"{key}={value}" for key, value in (labels.items() if labels is not None else [])])
        list_response = list_func(namespace, label_selector=label_str)

        if len(list_response.items) > 0:
            obj_name = list_response.items[0].metadata.name

            # List responses don't include the API version or kind of the object - so we need to make another request.
            obj = get_func(obj_name, namespace)
            self.log.info(f"Found an existing {obj.kind} for {obj_name}.")
            return obj

        self.log.info(f"Creating a new {body.kind} {body.api_version} ({body.metadata.name}) for {self.job_name}...")
        response = create_func(namespace, body)
        return response
