from datetime import datetime
import warnings
from pathlib import Path

from .source import Source


class RouteViews(Source):
    """Source for MRT RIB dumps from Route Views"""

    URL: str = "http://archive.routeviews.org"

    def get_urls(self, dl_time: datetime, requests_cache_dir: Path) -> tuple[str, ...]:
        """Gets URLs of MRT RIB dumps for route views"""

        assert dl_time.hour % 2 == 0, "route views only downloads every two hours"
        # Links to collectors
        links = [
            f"{self.URL}{x}/"
            for x in self._get_hrefs(requests_cache_dir)
            if "/bgpdata" in x
        ]
        if len(links) != 40:
            warnings.warn(f"Expected 40 collectors from route views, got {len(links)}")
        # Return the links to the dumps from the collector links
        return tuple(
            [dl_time.strftime(f"{x}%Y.%m/RIBS/rib.%Y%m%d.%H00.bz2") for x in links]
        )
