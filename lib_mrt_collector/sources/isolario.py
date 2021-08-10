from datetime import datetime
import logging

from lib_utils import helper_funcs

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
        collectors = [Isolario.url + x["href"] for x in
                      helper_funcs.get_tags(Isolario.url, 'a')][5:]

        return [t.strftime("{x}%Y_%m/rib.%Y%m%d.%H00.bz2") for x in collectors]
