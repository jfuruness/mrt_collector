"""This file contains functions used to dump BGP RIBs"""

from subprocess import check_call
from typing import Callable

from .mrt_file import MRTFile


PARSE_FUNC = Callable[[MRTFile], None]


def bgpkit_parser_json(mrt_file: MRTFile) -> None:
    """Extracts info from raw dumps into parsed path

    For this particular parser, I output both to CSV and to JSON
    I know it makes it take twice as long, but you need the CSV for the prefixes,
    and the JSON is just so convenient.

    You only have to run it once to analyze it any number of ways, so whatevs

    For other funcs of this kind, note that you MUST always pipe to PSV
    """

    # This takes up so much space that it's not even possible on 1 TB machine
    # if not mrt_file.parsed_path_psv.exists():
    #     check_call(
    #         f"bgpkit-parser {mrt_file.raw_path} > {mrt_file.parsed_path_psv}",
    #         shell=True,
    #     )
    if not mrt_file.parsed_path_json.exists():
        check_call(
            f"bgpkit-parser {mrt_file.raw_path} --json > {mrt_file.parsed_path_json}",
            shell=True,
        )


def bgpkit_parser(mrt_file: MRTFile) -> None:
    """Extracts info from raw dumps into parsed path"""

    if not mrt_file.parsed_path_psv.exists():
        check_call(
            f"bgpkit-parser {mrt_file.raw_path} > {mrt_file.parsed_path_psv}",
            shell=True,
        )
