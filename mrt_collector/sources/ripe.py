from datetime import datetime
import warnings
from pathlib import Path

from .source import Source


class RIPE(Source):
    """Source for MRT RIB dumps from Ripe"""

    URL: str = (
        "https://www.ripe.net/analyse/internet-measurements/"
        "routing-information-service-ris/ris-raw-data"
    )

    def get_urls(self, dl_time: datetime, requests_cache_dir: Path) -> tuple[str, ...]:
        """Gets URLs of MRT RIB dumps for RIPE/RIS"""

        assert dl_time.hour % 8 == 0, "RIPE/RIS only downloads RIBS every 8hrs"
        # Links to collectors
        links = [
            x
            for x in self._get_hrefs(requests_cache_dir)
            if x.startswith("http://data.ris.ripe.net/rrc")
        ]
        if len(links) != 26:
            warnings.warn(f"Expected 26 collectors from RIPE, got {len(links)}")
        # Return the links to the dumps from the collector links
        return tuple(
            [dl_time.strftime(f"{x}/%Y.%m/bview.%Y%m%d.%H00.gz") for x in links]
        )
