from ttd.jinja.template_builder import TemplateBuilder
from airflow.utils.xcom import XCOM_RETURN_KEY


class XComReturnValue:
    """
    Helper functions for getting a task's return value.
    """

    @staticmethod
    def template_builder(task_id: str) -> TemplateBuilder:
        """Returns Jinja template builder for a task's return value."""
        return TemplateBuilder(value=f"task_instance.xcom_pull('{task_id}', key='{XCOM_RETURN_KEY}')")
