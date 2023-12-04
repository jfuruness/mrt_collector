import contextlib
import time
import threading
from typing import Iterable

from tqdm import tqdm

from .mrt_file import MRTFile


@contextlib.contextmanager
def mrt_dl_pbar(mrt_files: Iterable[MRTFile]):
    """A progress bar showing which MRT files are downloaded in another thread"""

    thread = threading.Thread(target=mrt_dl_pbar_helper, args=(mrt_files,))
    thread.start()
    yield
    # Ensure the thread completes before exiting the context manager
    thread.join()



def mrt_dl_pbar_helper(mrt_files: Iterable[MRTFile]) -> None:
    """A progress bar showing which MRT files are downloaded"""

    def total_downloaded(mrt_files) -> int:
        return sum([int(x.downloaded) for x in mrt_files])


    with tqdm(
        total=len(mrt_files),
        desc="Downloading MRTs (~1hr)",
    ) as pbar:
        downloaded = total_downloaded(mrt_files)
        while downloaded < len(mrt_files):
            downloaded = total_downloaded(mrt_files)
            pbar.n = downloaded
            pbar.refresh()
            time.sleep(2)
