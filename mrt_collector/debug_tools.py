import json
from pathlib import Path

from .mrt_file import MRTFile


def ec_file_sizes_to_json(mrt_files: tuple[MRTFile, ...], output_path: Path) -> None:
    """
    Outputs result from mrt_file head requests
    Helpful when debugging to skip the 7 min wait
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = {mrt_file.url: mrt_file.ec_file_size for mrt_file in mrt_files}
    with output_path.open("w") as f:
        json.dump(data, f, indent=2)


def ec_file_sizes_from_json(mrt_files: tuple[MRTFile, ...], input_path: Path) -> None:
    """
    Reads mrt_files ec_file_sizes from json
    Helpful when debugging to skip the 7 min wait
    from mrt_file head requests
    """
    with input_path.open("r") as f:
        url_to_size = json.load(f)

    for mrt_file in mrt_files:
        if mrt_file.url in url_to_size:
            mrt_file._ec_file_size = url_to_size[mrt_file.url]  # noqa
