def construct_model_name(team_name: str, endpoint_group: str, endpoint_base_name: str) -> str:
    return f"{team_name}-{endpoint_group}-{endpoint_base_name}"
