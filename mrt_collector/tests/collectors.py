import enum


# File to hold some collectors for testing use, to keep the tester code clean
class Collectors(enum.Enum):
    collectors_0: dict[str, list[str]] = dict()
    collectors_1: dict[str, list[str]] = {"collectors[]": ["route-views2"]}
    collectors_2: dict[str, list[str]] = {
        "collectors[]": ["route-views.telxatl", "route-views2"]
    }
    collectors_3: dict[str, list[str]] = {
        "collectors[]": ["route-views.telxatl", "route-views2", "route-views6"]
    }
