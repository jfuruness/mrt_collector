import csv
import json
import os
import shutil
import subprocess
import time
import warnings
from pathlib import Path
from subprocess import check_call
from urllib.parse import quote

import requests

from .retry_session import RetrySession
from .sources import Source


class MRTFile:
    def __init__(
        self,
        url: str,
        source: Source,
        raw_dir: Path,
        parsed_dir: Path,
        parsed_line_count_dir: Path,
        expected_file_size: int = 0
    ) -> None:
        self.url: str = url
        self.source: Source = source
        self.raw_path: Path = raw_dir / self._url_to_fname(self.url)
        self.parsed_path_psv: Path = parsed_dir / self._url_to_fname(
            self.url, ext="psv"
        )
        self.parsed_line_count_path: Path = parsed_line_count_dir / self._url_to_fname(
            self.url, ext="txt"
        )
        self._expected_cmprsed_file_size: int = expected_file_size

    def __lt__(self, other) -> bool:
        """For sorting by file size

        This is useful when doing long operations with multiprocessing.
        By starting with the largest files first, it takes significantly less time
        """

        if isinstance(other, MRTFile):
            afs_comparison = self.compare_afs(other)
            if afs_comparison is not None:
                return afs_comparison

            return self._expected_cmprsed_file_size < other.expected_cmprsed_file_size

        return NotImplemented

    def compare_afs(self, other) -> bool | None:
        """Tries to compare two MRT Files actual file sizes, first based on parsed_path,
        and if that doesn't exist then with raw_path. Returns None if neither exist"""

        for path_attr in ["parsed_path_psv", "raw_path"]:

            self_path = getattr(self, path_attr)
            other_path = getattr(other, path_attr)

            if self_path.exists() and other_path.exists():
                # Check the file size, sort in descending order
                # That way largest files are done first
                # https://stackoverflow.com/a/2104107/8903959
                return self_path.stat().st_size < other_path.stat().st_size

        return None

    def fetch_expected_file_size(self) -> None:
        """Tries to set expected_file_size with a HEAD request"""

        try:
            with RetrySession() as session:
                with session.head(self.url, timeout=60) as r:
                    status_code = r.status_code
                    if status_code == 200:
                        self._expected_cmrpsed_file_size = r.headers.get('Content-Length', 0)
                        return
        except Exception as e:
            print(f"URL {self.url} : Head Request failed due to {e} {type(e)}")
            raise

    def download_raw(self, retries: int = 3) -> None:
        """Downloads the raw file if you haven't already"""

        if self.download_succeeded:
            return

        # I tried using proper backoff strategies, such as:
        # https://stackoverflow.com/a/35504626/8903959
        # But this actually doesn't capture incomplete read
        # errors in URL lib. So I need to write my own.
        succeeded = False
        for i in range(retries):
            try:
                succeeded = self.attempt_download_raw()
                if succeeded:
                    break
            except Exception as e:
                if i == retries - 1:
                    raise
            time.sleep((i + 1) * 10)

        if not succeeded and self.raw_path.exists():
            self.raw_path.unlink(missing_ok = True)

    def attempt_download_raw(self) -> bool:
        """Attempts to download the raw MRT file"""

        try:
            with requests.get(self.url, stream=True, timeout=60) as r:
                status_code = r.status_code 
                r.raise_for_status() # raises an error if we get bad status code
                #honestly probably should swap this out for new Retry Session anyway
                if status_code == 200:
                    with self.raw_path.open("wb") as f:
                        shutil.copyfileobj(r.raw, f)
                    return self.download_succeeded
        except Exception as e:
            print(f"URL {self.url} failed due to {e} {type(e)}")
            raise

        return False

    def validate_file_size(self) -> bool:
        """Returns true if expected_file_size is equal to actual file size.
        Assumes the filepath and file exist."""

        stat_info = self.raw_path.stat()
        actual_file_size = stat_info.st_size

        if actual_file_size == 0:
            raise NotImplementedError("Expected cmprsd size 0 at " + str(self.raw_path))

        return actual_file_size == self._expected_cmprsed_file_size

    def _url_to_fname(self, url: str, ext: str = "") -> str:
        """Converts a URL into a file name"""

        # precede with non_url so that bgp dump tools don't mistake it for one
        fname = "non_url" + quote(self.url).replace("/", "_")
        if ext:
            fname = fname.replace(".gz", ext).replace(".bz2", ext)
            # The base without the extension
            base_name = os.path.splitext(fname)[0]
            fname = f"{base_name}.{ext}"
        return fname

    def count_parsed_lines(self) -> int:
        if not self.parsed_path_psv.exists():
            return 0
        if self.parsed_line_count_path.exists():
            with self.parsed_line_count_path.open() as f:
                return int(f.read())
        else:
            command = f"wc -l {self.parsed_path_psv}"
            # Run the command
            result = subprocess.run(command, shell=True, text=True, capture_output=True)

            # Check if the command was successful
            if result.returncode != 0:
                print("Error running command:", result.stderr)
                raise Exception

            # Process the output to get the total number of lines
            output = result.stdout.strip()
            lines = output.split("\n")
            count = int(lines[-1].strip().split(" ")[0])
            with self.parsed_line_count_path.open("w") as f:
                # Remove header
                count = int(count) - 1
                f.write(str(count))
                return int(count)

    @property
    def expected_cmprsed_file_size(self) -> int:
        """Returns expected compressed file size in bytes"""

        return self._expected_cmprsed_file_size

    @property
    def download_succeeded(self) -> bool:
        """Returns true if the raw file exists and matches the expected size"""

        if not self.raw_path.exists():
            return False

        return self.validate_file_size()

        """Returns true if download errored out. Just checks first line"""
        """
        with self.raw_path.open("rb") as f:
            for line in f:
                return line != self.dl_err_str.encode("utf-8")
        raise NotImplementedError("Empty file?")
        """

    @property
    def total_parsed_lines(self) -> int:
        return self.count_parsed_lines()
