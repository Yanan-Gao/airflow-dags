from typing import Optional

from airflow.models import Variable

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.pod_generator import make_safe_label_value
from airflow.utils.context import Context

from kubernetes.client.models import V1LocalObjectReference

from ttd.kubernetes.pod_resources import PodResources
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado import tag_utils


class TtdKubernetesPodOperator(KubernetesPodOperator):
    ui_color: str = "#870F54"
    ui_fgcolor: str = "#fff"
    label_prefix: str = "ttd.org"
    """
    Execute a task in a Kubernetes pod.

    This TTD operator is a lightweight wrapper over the standard KubernetesPodOperator to provide some extra
    functionality.

    :param: name: Name of the pod we will create (plus a random suffix if random_name_suffix=True).
    :param: task_id: The name of the task as it will appear in Airflow.
    :param namespace: The namespace the pod will be created within.
    :param image: Docker image you wish to launch.
    :param resources: Resource request/limits. Optional only if you're providing a template file to pod_template_file
        with resources defined there instead.
    """

    def __init__(
        self,
        name: str,
        task_id: str,
        namespace: str,
        image: str,
        resources: Optional[PodResources] = None,
        service_name: Optional[str] = None,
        *args,
        **kwargs,
    ):
        self.name = name
        self.service_name = service_name

        super().__init__(
            name=name,
            task_id=task_id,
            namespace=namespace,
            image_pull_secrets=[V1LocalObjectReference(name="use-docker")],
            image=image,
            container_resources=resources.as_k8s_object() if resources else None,
            *args,
            **kwargs,
        )

    def execute(self, context: Context):

        team = context["task"].owner.upper()
        dag_id = context["dag"].dag_id
        task_name = context["task"].task_id
        base_url = Variable.get('BASE_URL', 'https://airflow2.gen.adsrvr.org/')
        env = TtdEnvFactory.get_from_system()

        annotations = {"scrum_team": team, "url": f"{base_url.rstrip('/')}/dags/{dag_id}"}

        labels = {
            "app.kubernetes.io/name": make_safe_label_value(self.name),  # type: ignore[arg-type]
            "app.kubernetes.io/managed-by": "airflow",
            f"{self.label_prefix}/team": make_safe_label_value(team),
            f"{self.label_prefix}/job": make_safe_label_value(dag_id),
            f"{self.label_prefix}/resource": "kubernetes",
            f"{self.label_prefix}/source": "airflow",
            f"{self.label_prefix}/environment": make_safe_label_value(str(env)),
            f"{self.label_prefix}/task": make_safe_label_value(task_name)
        }

        creator = tag_utils.get_creator()
        if creator is not None:
            labels[f"{self.label_prefix}/creator"] = make_safe_label_value(creator)

        if self.service_name is not None:
            labels["service"] = make_safe_label_value(self.service_name)

        self.labels.update(labels)
        self.annotations.update(annotations)

        return super().execute(context)
