from datetime import datetime
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from .mrt_file import MRTFile
from .sources import Source


class MRTCollector:
    def __init__(self, dl_time: datetime, base_dir: Optional[Path] = None) -> None:
        """Creates directories"""

        # Set base directory
        if base_dir is None:
            self.base_dir: Path = Path.home() / f"mrt_collector_{dl_time.date()}"
        else:
            self.base_dir = base_dir

        self._initialize_dirs()

    def run(
        self,
        sources: tuple[Source, ...] = Source.sources.copy(),
        # Steps
        mrt_files: Optional[tuple[MRTFile, ...]] = None,
        download_raw_mrts=True,
    ) -> None:
        """See README package description"""

        mrt_files = mrt_files if mrt_files else self.get_mrt_files(sources)
        if download_raw_mrts:
            self.download_raw_mrts(mrt_files)
        raise NotImplementedError

    def get_mrt_files(self, sources: tuple[Source, ...] = Source.sources.copy()) -> tuple[MRTFile, ...]:
        """Gets URLs from sources (cached) and returns MRT File objects"""

        mrt_files = list()
        for source in tqdm(sources, total=len(sources), desc=f"Parsing {sources}"):
            for url in source.get_urls(self.dl_time, self.requests_cache_dir):
                mrt_files.append(
                    MRTFile(
                        url,
                        source,
                        raw_dir=self.raw_dir,
                        parsed_dir=self.parsed_dir,
                        prefixes_dir=self.prefixes_dir,
                        formatted_dir=self.formatted_dir
                    )
                )
        return tuple(mrt_files)

    def download_raw_mrts(self, mrt_files: tuple[MRTFile, ...]) -> None:
        """Downloads raw MRT RIB dumps into raw_dir"""

        for mrt_file in mrt_files:
            mrt_file.download_raw()

    ###############
    # Directories #
    ###############

    def _initialize_dirs(self) -> None:
        """Initializes dirs. Does this now to prevent issues when multiprocessing"""

        for dir_ in (
            self.base_dir,
            self.requests_cache_dir,
            self.raw_dir,
            self.parsed_dir,
            self.prefixes_dir,
            self.formatted_dir,
            self.analysis_dir,
        ):
            dir_.mkdir(parents=True, exist_ok=True)

    @property
    def requests_cache_dir(self) -> Path:
        """Returns dir that is sometimes used to cache requests

        For example, when getting URLs from RIPE and Route Views,
        this directory is used
        """

        return self.base_dir / "requests_cache"

    @property
    def raw_dir(self) -> Path:
        """Returns directory into which raw MRTs are downloaded"""
        return self.base_dir / "raw"

    @property
    def parsed_dir(self) -> Path:
        """Directory in which MRTs are parsed using available tools"""
        return self.base_dir / "parsed"

    @property
    def prefixes_dir(self) -> Path:
        """Dir in which prefixes are extracted from MRTs"""
        return self.base_dir / "prefixes"

    @property
    def formatted_dir(self) -> Path:
        """Dir into which parsed files are formatted into useful CSVs"""
        return self.base_dir / "formatted"

    @property
    def analysis_dir(self) -> Path:
        """Returns directory into which analysis files are stored"""
        return self.base_dir / "analysis"
