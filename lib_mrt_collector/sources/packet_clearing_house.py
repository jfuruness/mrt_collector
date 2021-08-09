from .source import Source


class PacketClearingHouse(Source):
    """Source for MRT RIB dumps from packet clearing house"""

    url = ""
    value = 2

    @staticmethod
    def get_urls(dl_time):
        return []
