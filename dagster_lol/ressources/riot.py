import time
import urllib.parse
from typing import Any

import requests
from dagster import ConfigurableResource

MATCH_ID_PREFIX_TO_CLUSTER = {
    "EUW1": "europe",
    "EUN1": "europe",
    "TR1": "europe",
    "RU": "europe",
    "KR": "asia",
    "JP1": "asia",
    "NA1": "americas",
    "LA1": "americas",
    "LA2": "americas",
    "BR1": "americas",
    "OC1": "sea",
    "SG2": "sea",
    "PH2": "sea",
    "TW2": "sea",
    "VN2": "sea",
    "TH2": "sea",
}

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

    def get_match_ids(
        self,
        puuid: str,
        region: str,
        log: Any,
        total: int = 500,
    ) -> list[str]:
        """Fetch the last `total` ranked solo/duo match IDs for a given PUUID.

        Paginates automatically (max 100 per request from the Riot API).

        Args:
            puuid: Riot PUUID of the player.
            region: League region used to pick the correct regional cluster.
            log: Dagster context.log passed from the calling asset.
            total: Maximum number of match IDs to return (default 500).

        Returns:
            List of match ID strings, ordered most recent first.
        """
        cluster = REGION_TO_CLUSTER.get(region.upper(), DEFAULT_CLUSTER)
        url = f"https://{cluster}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"

        match_ids: list[str] = []
        page_size = 100

        for start in range(0, total, page_size):
            count = min(page_size, total - start)
            params = {
                "queue": 420,
                "start": start,
                "count": count,
                "api_key": self.api_key,
            }

            log.debug(
                "GET match ids for puuid=%s… (cluster=%s, start=%d, count=%d)",
                puuid[:8],
                cluster,
                start,
                count,
            )

            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    response = requests.get(url, params=params, timeout=10)
                    log.debug(
                        "  → HTTP %d [%dms] (start=%d, attempt=%d)",
                        response.status_code,
                        response.elapsed.microseconds // 1000,
                        start,
                        attempt,
                    )

                    if response.status_code == 200:
                        page = response.json()
                        match_ids.extend(page)
                        log.debug("  → %d match ids retrieved (total so far: %d)", len(page), len(match_ids))
                        if len(page) < count:
                            # No more matches available — stop pagination early
                            log.debug("  → Reached end of match history at start=%d", start)
                            return match_ids
                        break

                    if response.status_code == 404:
                        log.debug("  → No match history for puuid=%s…", puuid[:8])
                        return match_ids

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        log.warning(
                            "Rate limit hit (429) — waiting %ds (start=%d, attempt %d/%d)",
                            retry_after,
                            start,
                            attempt,
                            _MAX_RETRIES,
                        )
                        time.sleep(retry_after)
                        continue

                    response.raise_for_status()

                except requests.RequestException as exc:
                    log.warning(
                        "Request failed for puuid=%s… start=%d (attempt %d/%d): %s",
                        puuid[:8],
                        start,
                        attempt,
                        _MAX_RETRIES,
                        exc,
                    )
                    if attempt < _MAX_RETRIES:
                        time.sleep(2 ** attempt)
            else:
                log.warning(
                    "All %d attempts failed for puuid=%s… start=%d — stopping pagination",
                    _MAX_RETRIES,
                    puuid[:8],
                    start,
                )
                return match_ids

        return match_ids

    def get_match(self, match_id: str, log: Any) -> dict | None:
        """Fetch the full details of a match.

        The regional cluster is inferred from the match_id prefix (e.g. "EUW1_xxx" → europe).

        Args:
            match_id: Riot match ID (e.g. "EUW1_7819128666").
            log: Dagster context.log passed from the calling asset.

        Returns:
            Dict with full match data, or None if not found or request failed.
        """
        prefix = match_id.split("_")[0]
        cluster = MATCH_ID_PREFIX_TO_CLUSTER.get(prefix, DEFAULT_CLUSTER)
        url = f"https://{cluster}.api.riotgames.com/lol/match/v5/matches/{match_id}"

        log.debug("GET match %s (cluster=%s)", match_id, cluster)

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = requests.get(url, params={"api_key": self.api_key}, timeout=10)
                log.debug(
                    "  → HTTP %d [%dms] for %s (attempt %d)",
                    response.status_code,
                    response.elapsed.microseconds // 1000,
                    match_id,
                    attempt,
                )

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 404:
                    log.debug("  → Match not found: %s", match_id)
                    return None

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    log.warning(
                        "Rate limit hit (429) for %s — waiting %ds (attempt %d/%d)",
                        match_id,
                        retry_after,
                        attempt,
                        _MAX_RETRIES,
                    )
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()

            except requests.RequestException as exc:
                log.warning(
                    "Request failed for %s (attempt %d/%d): %s",
                    match_id,
                    attempt,
                    _MAX_RETRIES,
                    exc,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(2 ** attempt)

        log.warning("All %d attempts failed for %s — returning None", _MAX_RETRIES, match_id)
        return None
