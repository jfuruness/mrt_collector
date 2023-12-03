from datetime import datetime
import logging

from lib_utils.helper_funcs import get_hrefs

from .source import Source


class RouteViews(Source):
    """Source for MRT RIB dumps from Route Views"""

    url = "http://archive.routeviews.org"

    @staticmethod
    def get_urls(t: datetime):
        """Gets URLs of MRT RIB dumps for Isolario"""

        logging.debug("Getting Route Views URLs")

        assert t.hour % 2 == 0, "RouteViews only downloads ribs every 2hrs"

        # Links to the collectors
        urls = [RouteViews.url + x + "/" for x in get_hrefs(RouteViews.url)
                if "/bgpdata" in x]
        # Links to the RIB dumps
        return [t.strftime(f"{x}%Y.%m/RIBS/rib.%Y%m%d.%H00.bz2") for x in urls]
