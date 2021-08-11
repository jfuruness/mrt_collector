from datetime import datetime
import logging

from lib_utils.helper_funcs import get_hrefs

from .source import Source


class Isolario(Source):
    """Source of MRT RIB dumps for Isolario"""

    url = "http://isolario.it/Isolario_MRT_data/"
    value = 3

    @staticmethod
    def get_urls(t: datetime) -> list:
        """Gets URLs of MRT RIB dumps for Isolario"""

        logging.debug("Getting Isolario URLs")
        # Inspired by code written by Matt Jaccino originally

        assert t.hour % 2 == 0, "Isolario only downloads ribs every 2hrs"
        # Gets all the collectors urls
        urls = [Isolario.url + x for x in get_hrefs(Isolario.url)][5:]

        return [t.strftime(f"{x}%Y_%m/rib.%Y%m%d.%H00.bz2") for x in urls]
