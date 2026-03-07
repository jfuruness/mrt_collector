"""Funcs that parse rib dumps"""

from subprocess import check_call
from typing import Callable

from .mrt_file import MRTFile

PARSE_FUNC = Callable[[MRTFile], None]


def bgpkit_parser(mrt_file: MRTFile) -> None:
    """Extracts info from raw dumps into parsed path"""

    check_call( # noqa
        # need the single quotes for the entire string and double quotes for the paths
        # to tell the shell to treat everything as a single path
        f'bgpkit-parser "{mrt_file.raw_path}" --psv > "{mrt_file.parsed_path_psv}"',
        shell=True,
    )
