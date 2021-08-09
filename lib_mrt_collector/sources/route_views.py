from .source import Source


class RouteViews(Source):
    """Source for MRT RIB dumps from Route Views"""

    url = ""
    value = 1

    @staticmethod
    def get_urls():
        pass
