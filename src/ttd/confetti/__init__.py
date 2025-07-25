try:  # optional import to avoid heavy airflow dependency in simple contexts
    from .auto_configured_emr_job_task import AutoConfiguredEmrJobTask
except Exception:  # pragma: no cover - airflow may not be installed
    AutoConfiguredEmrJobTask = None  # type: ignore

from .confetti_task_factory import make_confetti_tasks, make_confetti_post_processing_task

__all__ = ["AutoConfiguredEmrJobTask", "make_confetti_tasks", "make_confetti_post_processing_task"]
