import json
from pathlib import Path
import os
import shutil
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
        prefixes_dir: Path,
        formatted_dir: Path,
    ) -> None:
        self.url: str = url
        self.source: Source = source
        self.raw_path: Path = raw_dir / self._url_to_fname(self.url)
        self.parsed_path_psv: Path = parsed_dir / self._url_to_fname(
            self.url, ext="psv"
        )
        # NOTE: This isn't always filled with a file, but is sometimes useful
        self.parsed_path_json: Path = parsed_dir / self._url_to_fname(
            self.url, ext="json"
        )
        self.non_unique_prefixes_path: Path = prefixes_dir / self._url_to_fname(
            self.url + "_non_unique", ext="csv"
        )
        self.unique_prefixes_path: Path = prefixes_dir / self._url_to_fname(
            self.url + "_unique", ext="csv"
        )

        # self.formatted_path: Path = formatted_dir

    def __lt__(self, other) -> bool:
        """For sorting by file size

        This is useful when doing long operations with multiprocessing.
        By starting with the largest files first, it takes significantly less time
        """

        if isinstance(other, MRTFile):
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
        return NotImplemented

    def download_raw(self, retries: int = 3) -> None:
        """Downloads the raw file if you haven't already"""

        if self.downloaded:
            return

        # I tried using proper backoff strategies, such as:
        # https://stackoverflow.com/a/35504626/8903959
        # But this actually doesn't capture incomplete read
        # errors in URL lib. So I need to write my own.
        status_code = 0
        for i in range(retries):
            try:
                with requests.get(self.url, stream=True, timeout=60) as r:
                    status_code = r.status_code  # type: ignore
                    if status_code == 200:
                        with self.raw_path.open("wb") as f:
                            shutil.copyfileobj(r.raw, f)  # type: ignore
                            return
            except Exception as e:
                if status_code == 404:
                    print(f"URL {self.url} failed due to 404 {i + 1}/{retries}")
                else:
                    print(
                        f"URL {self.url} failed due to {e} {type(e)} {i + 1}/{retries}"
                    )
                if i == retries - 1:
                    raise

            time.sleep((i + 1) * 10)

        # Don't error, sometiems files fail due to not found errors (404)
        warnings.warn(
            f"status of {status_code} for {self.url}, download failed but didn't err"
        )
        # Write the file so that you don't go back to it later
        with self.raw_path.open("wb") as f:
            f.write(self.dl_err_str.encode("utf-8"))

    def store_unique_prefixes(self) -> None:
        """Stores unique prefixes if not previously stored"""

        if not self.unique_prefixes_path.exists() and self.parsed_path_psv.exists():
            # unique instead of awk here because it's sometimes ribs in
            # so may prefix origin pairs are next to each other
            # By adding uniq here. mrt_collector._get_prefix_ids has a 3x speedup
            # Even the bash cmd speeds up because it doesn't write as much
            check_call(
                f'cut -d "|" -f 2 {self.parsed_path_psv} | uniq > {self.prefixes_path}',
                shell=True,
            )
        elif not self.unique_prefixes_path.exists() and self.parsed_path_json.exists():
            with self.non_unique_prefixes_path.open("w") as prefixes_f:
                with self.parsed_path_json.open() as json_f:
                    for line in json_f:
                        prefixes_f.write(json.loads(line)["prefix"] + "\n")
            # make this unique using cat, that way it's not slow Python
            # makes joining these files in MRTCollector 3x faster
            check_call(
                (
                    f"cat {self.non_unique_prefixes_path} "
                    f"| uniq > {self.unique_prefixes_path}"
                ),
                shell=True,
            )

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

    @property
    def downloaded(self) -> bool:
        """Returns True if file downloaded else False"""

        return self.raw_path.exists()

    @property
    def download_succeeded(self) -> bool:
        """Returns true if download errored out. Just checks first line"""

        with self.raw_path.open("rb") as f:
            for line in f:
                return line != self.dl_err_str.encode("utf-8")
        raise NotImplementedError("Empty file?")

    @property
    def dl_err_str(self) -> str:
        """String that is stored within a file if download errors"""

        return "ERROR"
