import csv
import json
from pathlib import Path
import os
import shutil
import subprocess
from subprocess import check_call
import time
from urllib.parse import quote
import warnings

import requests

from .sources import Source


class MRTFile:
    def __init__(
        self,
        url: str,
        source: Source,
        raw_dir: Path,
        parsed_dir: Path,
        parsed_line_count_dir: Path
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
        self._expected_file_size: int = expected_file_size #defaults to zero for now

    def __lt__(self, other) -> bool:
        """For sorting by file size

        This is useful when doing long operations with multiprocessing.
        By starting with the largest files first, it takes significantly less time
        """

        
        if isinstance(other, MRTFile):
            return self._expected_file_size < other.expected_file_size
        """
            for path_attr in ["parsed_path_psv", "raw_path"]:
                # Save the paths to variables
                self_path = getattr(self, path_attr)
                other_path = getattr(other, path_attr)
                # If both parsed paths exist
                if self_path.exists() and other_path.exists():
                    # Check the file size, sort in descending order
                    # That way largest files are done first
                    # https://stackoverflow.com/a/2104107/8903959
                    if self_path.stat().st_size > other_path.stat().st_size:
                        return True
                    else:
                        return False
        """
        return NotImplemented
 
    def fetch_expected_file_size(self) -> None:
        """Tries to set expected_file_size with a HEAD request"""
         
        try:
            with requests.head(self.url, timeout=60) as r:
                status_code = r.status_code
                if r.status_code == 200:
                    self._expected_file_size = response.headers.get('Content-Length', 0))
        except Exception as e:
            print(f"URL {self.url} : Head Request failed due to {e} {type(e)}")
            raise
            # not sure if we want to raise an exception here, or just throw out this file, for now I'll raise
            # the more I look at this the more I just need to add retry logic,
            # and if all retries are exceeded then we just need to remove this file, not kill the entire
            # program. This will, however, require me to rework the set_all_expected_sizes func
            # because it currently returns nothing, so we have no current way to communicate
            # which files need to be removed. maybe I'll create a separate tuple of files_to_remove and
            # create a new tuple [MRTFiles for x in MRTFiles not in files_toRemove]

    def download_raw(self, retries: int = 3) -> None:
        """Downloads the raw file if you haven't already"""

        if self.download_succeded or self.downloaded: # I question if we still want this here
            return False

        # I tried using proper backoff strategies, such as:
        # https://stackoverflow.com/a/35504626/8903959
        # But this actually doesn't capture incomplete read
        # errors in URL lib. So I need to write my own.
        status_code = 0
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
        
        if not succeeded and self.downloaded:
            self.raw_path.unlink(missing_ok = True)

        return succeeded # in my mind we should propogate back up and
                         # use this information to pop this file off the list
        
        # below logic should no longer be neccesary

        # Don't error, sometiems files fail due to not found errors (404)
        # warnings.warn(
        #     f"status of {status_code} for {self.url}, download failed but didn't err"
        # )

        # below logic should no longer be neccesary

        # Write the file so that you don't go back to it later
        # with self.raw_path.open("wb") as f:
        #    f.write(self.dl_err_str.encode("utf-8"))

    def attempt_download_raw(self) -> bool:
        """Attempts to download the raw MRT file"""

        try:
            with requests.get(self.url, stream=True, timeout=60) as r:
                status_code = r.status_code  # type: ignore
                r.raise_status # raises an error if we get a less than ideal status code
                if status_code == 200:
                    with self.raw_path.open("wb") as f:
                        shutil.copyfileobj(r.raw, f)  # type: ignore
                    return self.download_succeeded
         except Exception as e:
            print(f"URL {self.url} failed due to {e} {type(e)} {i + 1}/{retries}")
            raise

    def validate_file_size(self) -> bool:
        """Returns true if expected_file_size is equal to actual file size. Assumes the filepath and file exist."""
        stat_info = self.raw_path.stat()
        actual_file_size = stat_info.st_size

        if actual_file_size == 0:
            return False

        return actual_file_size == self._expected_file_size

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
    def expected_file_size(self) -> int:
        """Returns expected file size in bytes"""

        return self._expected_file_size
         
    @property
    def downloaded(self) -> bool:
        """Returns True if file downloaded else False"""

        return self.raw_path.exists()

    @property
    def download_succeeded(self) -> bool:
        """Returns true if the raw file exists and matches the expected size"""

        if not self.downloaded:
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
    def dl_err_str(self) -> str:
        """String that is stored within a file if download errors"""

        return "ERROR"

    @property
    def total_parsed_lines(self) -> int:
        return self.count_parsed_lines()
