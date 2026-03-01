import csv
from .mrtfile import MRTFile
from pathlib import Path
import typing

def ec_file_sizes_to_csv(
    mrt_files: tuple[MRTFile, ...],
    output_path: Path
) -> None:
    """
    Outputs result from mrt_file head requests
    Helpful when debugging to skip the 7 min wait
    """

    with output_oath.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["url", "ec_file_size"])
        for mrt_file in mrt_files:
            writer.writerow([mrt_file.url, mrt_file.ec_file_size])

def ec_file_sizes_from_csv(
    mrt_files: tuple["MRTFile", ...],
    input_path: Path
) -> None:
    """
    Reads mrt_files ec_file_sizes from csv
    Helpful when debugging to skip the 7 min wait
    from mrt_file head requests
    """

    with input_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        url_to_size = {
            row["url": int(row["ec_file_size"]) for row in reader
        }

    for mrt_file in mrt_files:
        if mrt_file.url in url_to_size:
            mrt_file._ec_file_size = url_to_size[mrt_file.url]
