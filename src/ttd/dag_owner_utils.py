import sys
import traceback
import os


def get_team_from_filelocation(file_location: str) -> str | None:
    components = os.path.normpath(file_location).split(os.path.sep)
    if "dags" in components:
        team_index = components.index("dags") + 1
        if ".py" in components[team_index]:
            raise ValueError(
                f"DAG at {file_location} is placed outside of the dags directory and not within a team directory"
                "please place the dag within your teams directory"
            )
        return components[team_index]
    return None


def infer_team_from_call_stack() -> str | None:
    """
    The base airflow DAG implementation calls `sys._getframe().f_back.f_code.co_filename` to get the file location (DAG.fileloc).
    In our dag policy enforcement we call `fetch_dag_owner_from_file_location` with that file location to figure out the team.
    The problem is that since TtdDag wraps DAG, we can't extract that during `_adopt_ttd_dag` because the dag location becomes the source for TtdDags

    So for this, we call the same `sys._getframe().f_back` and walk the stack upward, grabbing the highest level team name to return.

    If no team is matched, like in unit tests, it returns an empty string. This is because otherwise the unit tests will fail.
    """
    team = None
    fstart = sys._getframe().f_back

    if fstart is not None:
        for frame, _ in traceback.walk_stack(fstart):
            floc = frame.f_code.co_filename
            loc_team = get_team_from_filelocation(floc)
            if loc_team is not None:
                # We want to get the highest level stack that has a team associated with it
                team = loc_team

    return team
