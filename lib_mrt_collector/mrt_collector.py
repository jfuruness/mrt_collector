from datetime import datetime
from pathlib import Path
from multiprocessing import cpu_count
from typing import List, Optional

from .sources import Source


class MRTCollector(Base):
    """This class downloads, parses, and stores MRT Rib dumps

    NOTE: this library uses https://git.doublefourteen.io/bgp/ubgpsuite
    The creator of bgpscanner moved on to this library since Isolario
    reached it's end of life. bgpscanner contained bugs that were never
    fixed
    """

    def __init__(self,
                 base_dir: Path = Path("/tmp/mrt_collector"),
                 parse_cpus: int = cpu_count() - 1,
                 debug: bool = False,
                 dl_time: Optional[datetime] = None,
                 sources: List[Source] = Source.sources.copy()):
        """Sets instance vars and paths"""

        self.debug: bool = debug
        self.parse_cpus: int = parse_cpus
        if dl_time:
            self.dl_time: datetime = dl_time
        else:
            self.dl_time = self._default_dl_time

        self.sources: Tuple[Source, ...] = tuple(sources)

        self.base_dir: Path = base_dir
        self.raw_dir: Path = base_dir / "raw"
        self.dumped_dir: Path = base_dir / "dumped"
        self.prefix_dir: Path = base_dir / "prefix"
        self.parsed_dir: Path = base_dir / "parsed"

        for path in [self.raw_dir,
                     self.dumped_dir,
                     self.prefix_dir,
                     self.parsed_dir]:
            path.mkdir(parents=True,
                       exist_ok=True)
