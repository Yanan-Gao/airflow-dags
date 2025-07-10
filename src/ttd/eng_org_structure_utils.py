import logging
from typing import Dict, Any, Optional

import requests
from cachetools import TTLCache, cached

from ttd.monads.maybe import Maybe, Just, Nothing

ENG_ORG_STRUCTURE_API = "https://eng-teams.gen.adsrvr.org/api/v1"
cache: TTLCache = TTLCache(maxsize=10 * 1024, ttl=24 * 60 * 60, getsizeof=len)


@cached(cache=cache)
def get_scrum_teams() -> Dict[str, Any]:
    response = requests.get(ENG_ORG_STRUCTURE_API + "/teams", timeout=30, verify='/usr/lib/ssl/certs/ttd-root-ca.pem')
    return response.json()


def warm_scrum_teams_cache() -> None:
    try:
        logging.info("Warming up scrum teams cache...")
        get_scrum_teams()
        logging.info("Scrum teams cache warmed")
    except Exception as e:
        logging.warning(f"Scrum teams cache warm-up failed: {e}")


def is_valid_team(name: str) -> bool:
    try:
        teams = get_scrum_teams()
        return name in teams
    except Exception as e:
        logging.warning(f"Error retrieving data for {name}: {e}. Assuming team is valid")
        return True


def is_user_in_team(name: str, team: str) -> bool:
    try:
        teams = get_scrum_teams()
        members = teams.get(team, {}).get("members", [])
        return any(name == member["alias"] or name == member.get("gitlab_username") for member in members)
    except Exception as e:
        logging.warning(f"Error retrieving data for {name}: {e}. Assuming user is in team")
        return True


def get_alarm_channel_id(team_name: Optional[str]) -> Optional[str]:
    if team_name is None:
        return None

    try:
        teams = get_scrum_teams()
        channel_id = (teams.get(team_name, {}).get("team", {}).get("slack", {}).get("channels", {}).get("alerts_id"))
        return channel_id
    except Exception as e:
        logging.warning(f"Failed to get Slack alarm channel id for team name = {team_name}: {e}")
    return None


def _find_user(name: str) -> Maybe[dict[str, str]]:
    teams = get_scrum_teams()
    for team_info in teams.values():
        for member in team_info.get("members", []):
            if name in {member.get("alias"), member.get("gitlab_username")}:
                return Just(member)
    return Nothing()


def is_valid_user(name: str) -> bool:
    try:
        user = _find_user(name)
        return user.is_just()
    except Exception as e:
        logging.warning(f"Error retrieving data for {name}: {e}. Assuming user is valid")
        return True


def get_slack_user_id(name: str) -> Maybe[str]:
    try:
        user = _find_user(name)
        return user.map(lambda member: member["slack_user_id"])
    except Exception as e:
        logging.info(f"Error retrieving slack user ID for {name}: {e}")
        return Nothing()
