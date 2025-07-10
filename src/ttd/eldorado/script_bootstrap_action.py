from typing import List, Optional


class ScriptBootstrapAction:

    def __init__(self, path: str, args: Optional[List[str]] = None, name: str = "bootstrap"):
        if args is None:
            args = []

        self.Name = name
        self.ScriptBootstrapAction = {"Path": path, "Args": args}
