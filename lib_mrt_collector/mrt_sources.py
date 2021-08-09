from enum import Enum


class MRT_Sources(Enum):
    """Sources for MRT files"""

    RIPE = 0
    ROUTE_VIEWS = 1
    # Packet clearing house
    # Hopefully supported in the future
    # PCH = 2
    # End of life
    #ISOLARIO = 3
