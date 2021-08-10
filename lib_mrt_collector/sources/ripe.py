from datetime import datetime
import logging

from lib_utils.helper_funcs import get_hrefs

from .source import Source


class Ripe(Source):
    """Source for MRT RIB dumps from Ripe"""

    url = ("https://www.ripe.net/analyse/internet-measurements/"
           "routing-information-service-ris/ris-raw-data")
    value = 0

    @staticmethod
    def get_urls(t: datetime):
        """Gets URLs of MRT RIB dumps for RIPE/RIS"""

        logging.debug("Getting RIPE/RIS URLs")

        assert t.hour % 8 == 0, "RIPE/RIS only downloads RIBS every 8hrs"
        # Links to collectors
        links = [x for x in get_hrefs(Ripe.url)
                 if x.startswith("http://data.ris.ripe.net/rrc")]
        if len(links) != 26:
            logging.warning("Number of collectors in RIPE is off")
        # Return the links to the dumps from the collector links
        return [t.strftime(f"{x}/%Y.%m/bview.%Y%m%d.%H00.gz") for x in links]
