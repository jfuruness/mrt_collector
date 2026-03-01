import os
import shutil
import subprocess
import time
from pathlib import Path
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
        expected_compressed_file_size: int = 0,
        status: str = "unknown"
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
        self._ec_file_size: int = expected_compressed_file_size

    def fetch_ec_file_size(
        self,
    ) -> None:
        """Tries to set expected_file_size with a HEAD request"""

        try:
            with RetrySession() as session:
                with session.head(self.url, timeout=60) as r:
                    status_code = r.status_code
                    if status_code == 200:
                        self._ec_file_size = int(r.headers.get('Content-Length', 0))
                        self.status = "Ready for download"
                        return
        except Exception as e: # noqa
            print(f"URL {self.url} : Head Request failed due to {e} {type(e)}; source will be stripped from downloads") # noqa
            # if type(e) == requests.exceptions.HTTPError:
            # raise

    def download_raw(self, retries: int = 3) -> None:
        """Downloads the raw file if you haven't already"""

        if self.download_succeeded:
            return

        # need min 3 sec delay, else exceeds rate limit
        # I tried putting this in _mp_tqdm in mrtcollector
        # but it didn't help. Trying here now
#        time.sleep(5)

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
            except Exception as e: # noqa
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
        Assumes the filepath and file exist.
        """

        stat_info = self.raw_path.stat()
        actual_file_size = stat_info.st_size

        if actual_file_size == 0:
            print("houston we have a file size")
            raise NotImplementedError("Expected cmprsd size 0 at " + str(self.raw_path))

        result = actual_file_size == self._ec_file_size
        print(self.url + " : downloaded correctly= " + str(result))
        return result

    def _url_to_fname(self, url: str, ext: str = "") -> str:
        """Converts a URL into a file name"""

        # precede with non_url so that bgp dump tools don't mistake it for one
        fname = "non_url" + quote(self.url).replace("/", "_")
        if ext:
            fname = fname.replace(".gz", ext).replace(".bz2", ext)
            # The base without the extension
            base_name = os.path.splitext(fname)[0] # noqa
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
            result = subprocess.run( # noqa
                command,
                shell=True,
                text=True,
                capture_output=True,
                check=True
            )

            # Process the output to get the total number of lines
            output = result.stdout.strip()
            lines = output.split("\n")
            count = int(lines[-1].strip().split(" ")[0])
            with self.parsed_line_count_path.open("w") as f:
                # Remove header
                count = int(count) - 1
                f.write(str(count))
                return int(count)

    def __str__(self) -> str:
        """Temporary str override for debugging issues with sources"""

        temp = self.url

        efs = str(self.ec_file_size)
        if self.status == "Ready for download":
            temp += "\nExpected compressed file size= " + efs

        temp += "\nStatus= " + self.status + "\n----"

        return temp

    @property
    def download_succeeded(self) -> bool:
        """Returns true if the raw file exists and matches the expected size"""

        if not self.raw_path.exists():
            return False

        return self.validate_file_size()

    @property
    def ec_file_size(self) -> int:
        """Returns expected compressed file size in bytes"""

        return self._ec_file_size

    @property
    def ac_file_size(self) -> int:
        """Returns actual (post download) compressed file size in bytes"""

        if not self.raw_path.exists():
            raise ValueError("Actual file does not exist, from " + self.url)

        return self.raw_path.stat().st_size

    @property
    def parsed_file_size(self) -> int:
        """Returns parsed file size in bytes"""

        if not self.parsed_path_psv.exists():
            raise ValueError("Parsed file does not exist, from " + self.url)

        return self.parsed_path_psv.stat().st_size

    @property
    def total_parsed_lines(self) -> int:
        return self.count_parsed_lines()
