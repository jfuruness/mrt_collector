from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup


class Source(ABC):
    """Base class for a source for MRT RIB dumps"""

    sources: tuple["Source", ...] = ()

    # https://stackoverflow.com/a/43057166/8903959
    def __init_subclass__(cls, **kwargs):
        """Overrides initializing subclasses"""

        super().__init_subclass__(**kwargs)
        Source.sources.append(cls)

    def _get_hrefs(self, requests_cache_dir: Path) -> tuple[str, ...]:
        """Parses a URL and returns all the Hrefs for it"""

        with CachedSession(requests_cache_dir / "cache.db") as session:
            # Get the soup for the page
            resp = session.get(url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            resp.close()

        return tuple([a['href'] for a in soup.find_all('a', href=True)])

    @abstractmethod
    def get_urls(self, dl_time: datetime, requests_cache_dir: Path) -> tuple[str, ...]:
        """Gets URLs for MRT RIB dumps"""

        raise NotImplementedError

    @property
    @abstractmethod
    def URL(self) -> str:
        """Returns download URL"""

        raise NotImplementedError
