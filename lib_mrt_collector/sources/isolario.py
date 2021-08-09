from .source import Source


class Isolario(Source):
    """Source of MRT RIB dumps for Isolario"""

    url = ""
    value = 3

    @staticmethod
    def get_urls():
        pass
