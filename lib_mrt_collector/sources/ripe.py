from .source import Source

class Ripe(Source):
    """Source for MRT RIB dumps from Ripe"""

    url = ("https://www.ripe.net/analyse/internet-measurements/"
           "routing-information-service-ris/ris-raw-data")
    value = 0

    @staticmethod
    def get_urls():
        pass
