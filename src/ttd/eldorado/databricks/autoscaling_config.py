class DatabricksAutoscalingConfig:

    def __init__(self, min_workers: int, max_workers: int) -> None:
        self.min_workers = min_workers
        self.max_workers = max_workers
