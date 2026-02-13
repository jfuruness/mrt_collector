import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class RetrySession(requests.Session):
    def __init__(
        self,
        retries: int = 3,
        backoff_factor: float = .3,
        status_forcelist: (int, ...) = (500, 502, 503, 504),
        raise_for_status_codes: (int, ...) = (400, 401, 403, 404, 429),
    ):
        super().__init__()
        self.raise_for_status_codes = raise_for_status_codes

        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        self.mount("http://", adapter)
        self.mount("https://", adapter)

    def request(self, *args, **kwargs):
        """Wrapper function for error checking"""
        response = super().request(*args, **kwargs)
        if response.status_code in self.raise_for_status_codes:
            response.raise_for_status()
        return response

