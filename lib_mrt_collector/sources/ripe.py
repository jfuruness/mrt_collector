from datetime import datetime
import logging

from lib_utils import helper_funcs

from .source import Source


class Ripe(Source):
    """Source for MRT RIB dumps from Ripe"""

    url = ("https://www.ripe.net/analyse/internet-measurements/"
           "routing-information-service-ris/ris-raw-data")
    value = 0

    @staticmethod
    def get_urls(dl_time: datetime):
        """Gets URLs of MRT RIB dumps for RIPE/RIS"""

        logging.debug("Getting RIPE/RIS URLs")

        assert t.hour % 8 == 0, "RIPE/RIS only downloads RIBS every 8hrs"
        collectors = [Ripe.url + x["href"] for x in
                      helper_funcs.get_tags(Ripe.url, "a")]
        input(collectors)
        return []
