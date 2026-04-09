import time
import urllib.parse
from typing import Any

import requests
from dagster import ConfigurableResource

REGION_TO_CLUSTER = {
    "EUW": "europe",
    "EUNE": "europe",
    "TR": "europe",
    "RU": "europe",
    "KR": "asia",
    "JP": "asia",
    "NA": "americas",
    "LAN": "americas",
    "LAS": "americas",
    "BR": "americas",
    "OCE": "sea",
    "SG": "sea",
    "PH": "sea",
    "TW": "sea",
    "VN": "sea",
    "TH": "sea",
}

DEFAULT_CLUSTER = "europe"

_MAX_RETRIES = 3


class RiotResource(ConfigurableResource):
    api_key: str

    def get_puuid(self, game_name: str, tag_line: str | None, region: str, log: Any) -> str | None:
        """Fetch the PUUID for a Riot account.

        Args:
            game_name: Riot game name (e.g. "MISA 113").
            tag_line: Riot tag line (e.g. "Kurii"). Falls back to "EUW" if empty.
            region: League region used to pick the correct regional cluster.
            log: Dagster context.log passed from the calling asset.

        Returns:
            The PUUID string, or None if the account was not found or the request failed.
        """
        resolved_tag = tag_line if tag_line else "EUW"
        cluster = REGION_TO_CLUSTER.get(region.upper(), DEFAULT_CLUSTER)

        encoded_game_name = urllib.parse.quote(game_name, safe="")
        encoded_tag_line = urllib.parse.quote(resolved_tag, safe="")
        url = (
            f"https://{cluster}.api.riotgames.com/riot/account/v1/accounts"
            f"/by-riot-id/{encoded_game_name}/{encoded_tag_line}"
        )

        log.debug("GET %s#%s (cluster=%s)", game_name, resolved_tag, cluster)

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = requests.get(url, params={"api_key": self.api_key}, timeout=10)
                log.debug(
                    "  → HTTP %d [%dms] for %s#%s (attempt %d)",
                    response.status_code,
                    response.elapsed.microseconds // 1000,
                    game_name,
                    resolved_tag,
                    attempt,
                )

                if response.status_code == 200:
                    puuid = response.json().get("puuid")
                    log.debug("  → PUUID: %s…", puuid[:8] if puuid else "None")
                    return puuid

                if response.status_code == 404:
                    log.debug("  → Account not found: %s#%s", game_name, resolved_tag)
                    return None

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    log.warning(
                        "Rate limit hit (429) for %s#%s — waiting %ds (attempt %d/%d)",
                        game_name,
                        resolved_tag,
                        retry_after,
                        attempt,
                        _MAX_RETRIES,
                    )
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()

            except requests.RequestException as exc:
                log.warning(
                    "Request failed for %s#%s (attempt %d/%d): %s",
                    game_name,
                    resolved_tag,
                    attempt,
                    _MAX_RETRIES,
                    exc,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(2 ** attempt)

        log.warning("All %d attempts failed for %s#%s — returning None", _MAX_RETRIES, game_name, resolved_tag)
        return None
