from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from requests_cache import CachedSession


class Source(ABC):
    """Base class for a source for MRT RIB dumps"""

    sources: tuple[type["Source"], ...] = ()

    # https://stackoverflow.com/a/43057166/8903959
    def __init_subclass__(cls, **kwargs):
        """Overrides initializing subclasses"""

        super().__init_subclass__(**kwargs)
        Source.sources += (cls,)

    def __repr__(self) -> str:
        return self.__class__.__name__

    def _get_hrefs(self, requests_cache_dir: Path) -> tuple[str, ...]:
        """Parses a URL and returns all the Hrefs for it"""

        with CachedSession(str(requests_cache_dir / "cache.db")) as session:
            # Get the soup for the page. Mypy also doesn't see this method
            resp = session.get(self.URL)  # type: ignore
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            resp.close()

        return tuple([a["href"] for a in soup.find_all("a", href=True)])

    @abstractmethod
    def get_urls(self, dl_time: datetime, requests_cache_dir: Path) -> tuple[str, ...]:
        """Gets URLs for MRT RIB dumps"""

        raise NotImplementedError

    @property
    @abstractmethod
    def URL(self) -> str:
        """Returns download URL"""

        raise NotImplementedError
