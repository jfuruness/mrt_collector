from datetime import datetime
import warnings
from pathlib import Path

from .source import Source


class RIPE(Source):
    """Source for MRT RIB dumps from Ripe"""

    # They updated their site to a new URL with only 23 links
    # URL: str = (
    #     "https://www.ripe.net/analyse/internet-measurements/"
    #     "routing-information-service-ris/ris-raw-data"
    # )
    URL: str = "https://ris.ripe.net/docs/route-collectors/#bgp-timer-settings"

    def get_urls(self, dl_time: datetime, requests_cache_path: Path) -> tuple[str, ...]:
        """Gets URLs of MRT RIB dumps for RIPE/RIS"""

        assert dl_time.hour % 8 == 0, "RIPE/RIS only downloads RIBS every 8hrs"
        prepended_url = "https://data.ris.ripe.net/rrc"
        # Links to collectors
        links = [
            x
            for x in self._get_hrefs(requests_cache_path)
            if x.startswith(prepended_url)
        ]
        # Listed as dead links on their website
        # TODO: don't hardcode this
        dead_links = [prepended_url + x for x in ("02/", "08/", "09/")]
        links = [x for x in links if x not in dead_links]
        if len(links) != 23:
            warnings.warn(f"Expected 23 collectors from RIPE, got {len(links)}")
        # Return the links to the dumps from the collector links
        return tuple(
            [dl_time.strftime(f"{x}/%Y.%m/bview.%Y%m%d.%H00.gz") for x in links]
        )
