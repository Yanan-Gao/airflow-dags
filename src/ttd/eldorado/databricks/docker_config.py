from typing import Optional, Dict, Union


class DockerAuth:
    username: str
    password: str

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def to_dict(self):
        return {"username": self.username, "password": self.password}


class DatabricksDockerImageOptions:
    url: str
    basic_auth: Optional[DockerAuth]

    def __init__(self, url: str, basic_auth: Optional[DockerAuth]):
        self.url = url
        self.basic_auth = basic_auth

    def to_dict(self) -> Dict[str, Union[str, Dict[str, str]]]:
        base_config = {"url": self.url}
        if self.basic_auth:
            base_config["basic_auth"] = self.basic_auth.to_dict()
        return base_config
