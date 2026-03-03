"""Funcs that parse rib dumps"""

from subprocess import check_call
import subprocess
from typing import Callable

from .mrt_file import MRTFile

PARSE_FUNC = Callable[[MRTFile], None]


def bgpkit_parser(mrt_file: MRTFile) -> None:
    """Extracts info from raw dumps into parsed path"""

    if not mrt_file.parsed_path_psv.exists():
        with mrt_file.parsed_path_psv.open("wb") as f:
            subprocess.run(
                ["bgpkit-parser", str(mrt_file.raw_path), "--psv"],
                stdout=f
            )

        return
        check_call( # noqa
            f"bgpkit-parser {mrt_file.raw_path} --psv > {mrt_file.parsed_path_psv}",
            shell=True,
        )
