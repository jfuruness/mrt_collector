from pathlib import Path
import os
import shutil
from urllib.parse import quote
import warnings

import requests
from requests.adapters import HTTPAdapter, Retry

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
        # self.parsed_path: Path = parsed_dir
        # self.prefixes_path: Path = prefixes_dir
        # self.formatted_path: Path = formatted_dir

    def download_raw(self) -> None:
        """Downloads the raw file if you haven't already"""

        if not self.downloaded:
            # https://stackoverflow.com/a/35504626/8903959
            with requests.Session() as session:
                retries = Retry(total=5,
                                backoff_factor=0.1,
                                status_forcelist=[ 500, 502, 503, 504 ])

                session.mount('http://', HTTPAdapter(max_retries=retries))
                session.mount('https://', HTTPAdapter(max_retries=retries))

                # https://stackoverflow.com/a/39217788/8903959
                # stream works best for large files
                with session.get(self.url, stream=True, timeout=60) as r:
                    if r.status_code == 200:
                        with self.raw_path.open("wb") as f:
                            shutil.copyfileobj(r.raw, f)  # type: ignore
                    # Don't error, some files always fail to download
                    else:
                        warnings.warn(f"status of {r.status_code} for {self.url}")

    def _url_to_fname(self, url: str, ext: str = "") -> str:
        """Converts a URL into a file name"""

        fname = quote(self.url).replace("/", "_")
        if ext:
            fname = fname.replace(".gz", ext).replace(".bz2", ext)
            # The base without the extension
            base_name = os.path.splitext(fname)[0]
            fname = f"{base_name}.csv"
        return fname

    @property
    def downloaded(self) -> bool:
        """Returns True if file downloaded else False"""

        return self.raw_path.exists()
