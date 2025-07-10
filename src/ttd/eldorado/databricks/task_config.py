from __future__ import annotations
from enum import Enum
from typing import Dict, Set, Optional, Union, List, Any
from abc import ABC, abstractmethod


class RunCondition(Enum):
    ALL_SUCCESS = "ALL_SUCCESS"
    ALL_DONE = "ALL_DONE"
    AT_LEAST_ONE_FAILED = "AT_LEAST_ONE_FAILED"


class TaskLocationConfig(ABC):
    type: str

    @abstractmethod
    def to_dict(self) -> Dict[str, Union[str, bool, List[str]]]:
        pass

    @abstractmethod
    def get_library_config(self) -> Optional[Dict[str, str]]:
        pass


class PythonWheelTaskLocation(TaskLocationConfig):
    type: str = "python_wheel_task"
    package_name: str
    entry_point: str
    parameters: List[str]

    def __init__(self, package_name: str, entry_point: str, parameters: List[str]):
        self.package_name = package_name
        self.entry_point = entry_point
        self.parameters = parameters

    def to_dict(self) -> Dict[str, Union[str, bool, List[str]]]:
        return {"package_name": self.package_name, "entry_point": self.entry_point, "parameters": self.parameters}

    def get_library_config(self) -> Optional[Dict[str, str]]:
        return None


class SparkJarTaskLocation(TaskLocationConfig):
    type: str = "spark_jar_task"
    main_class_name: str

    def __init__(self, class_name: str, jar_location: str, parameters: List[str], uses_eldorado_core: bool = True):
        self.class_name = class_name
        self.jar_location = jar_location
        self.parameters = parameters

    def to_dict(self) -> Dict[str, Union[str, bool, List[str]]]:
        return {"jar_uri": "", "main_class_name": self.class_name, "run_as_repl": True, "parameters": self.parameters}

    def get_library_config(self) -> Dict[str, str]:
        return {"jar": self.jar_location}


class SparkPythonTaskLocation(TaskLocationConfig):
    type: str = "spark_python_task"
    python_file: str
    params: List[str]

    def __init__(self, s3_path: str, params: List[str]):
        self.python_file = s3_path
        self.params = params

    def to_dict(self) -> Dict[str, Union[str, bool, List[str]]]:
        return {"python_file": self.python_file, "parameters": self.params}

    def get_library_config(self) -> Optional[Dict[str, str]]:
        return None


class SparkNotebookTaskLocation(TaskLocationConfig):
    type: str = "notebook_task"
    notebook_path: str
    base_parameters: Dict[str, Any]

    def __init__(self, notebook_path: str, base_parameters: Dict[str, Any]):
        self.notebook_path = notebook_path
        self.base_parameters = base_parameters

    def to_dict(self) -> Dict[str, Any]:
        return {"notebook_path": self.notebook_path, "base_parameters": self.base_parameters}

    def get_library_config(self) -> Optional[Dict[str, str]]:
        return None


class DatabricksTaskConfig:
    task_key: str
    run_if: RunCondition
    task_location: TaskLocationConfig
    cluster_key: str
    depends_on: Set[str]
    jar_library_s3_paths: Optional[List[str]]
    whl_paths: Optional[List[str]]

    def __init__(
        self,
        task_key: str,
        run_if: RunCondition,
        task_location: TaskLocationConfig,
        cluster_key: str,
        other_jar_s3_paths: Optional[List[str]] = None,
        whl_paths: Optional[List[str]] = None,
        depends_on: Optional[Set[str]] = None,
    ):
        self.task_key = task_key
        self.run_if = run_if
        self.task_location = task_location
        self.cluster_key = cluster_key
        self.jar_library_s3_paths = other_jar_s3_paths
        self.whl_paths = whl_paths
        self.depends_on = depends_on if depends_on is not None else set()

    def depend_on(self, other_task: DatabricksTaskConfig) -> None:
        self.depends_on.add(other_task.task_key)

    def to_dict(self) -> Dict[str, Union[str, bool, List[str]]]:
        task = {
            "task_key": self.task_key,
            self.task_location.type: self.task_location.to_dict(),
            "job_cluster_key": self.cluster_key,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notifications_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False
            },
            "webhook_notifications": {}
        }
        if len(self.depends_on) > 0:
            task['depends_on'] = [{"task_key": upstream_task_key} for upstream_task_key in self.depends_on]
            task['run_if'] = self.run_if.value
        # Determine which libraries should be loaded onto the task
        task_library_config = self.task_location.get_library_config()
        libraries = []
        if task_library_config is not None:
            libraries.append(task_library_config)
        if self.jar_library_s3_paths is not None:
            other_libraries = [{"jar": path} for path in self.jar_library_s3_paths]
            libraries.extend(other_libraries)
        if self.whl_paths is not None:
            other_libraries = [{"whl": path} for path in self.whl_paths]
            libraries.extend(other_libraries)
        if len(libraries) > 0:
            task["libraries"] = libraries
        return task
