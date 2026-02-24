import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RetrySession(requests.Session):
    def __init__(
        self,
        retries: int = 3,
        backoff_factor: float = .3,
        retry_for_status_codes: tuple[int, ...] = (500, 502, 503, 504),
        raise_for_status_codes: tuple[int, ...] = (400, 401, 403, 404, 429),
    ):
        super().__init__()
        self.raise_for_status_codes = raise_for_status_codes

        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=retry_for_status_codes,
            allowed_methods=["GET", "HEAD"],
            raise_on_status=True, # raises if all retries exhausted
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

"""
This is a subclass of requests.session created for
convenience and to prevent bloating
MRTFile.fetch_expected_file_size; within fetch... we
use this as a context manager. This class does two
things:

The first is that it mounts a Retry Adapter
to our Session in the object init, so we automatically
retry N amount of times in the event of a retry-demanding
return code (codes specificed in retry_for_status_codes).
Based on code from below link:
stackoverflow.com/questions/49121365/implementing-retry-for-requests-in-python/49121508#49121508

The second is that it overrides Session.request, so that we
do status error filtering outside of MRTFile. If our
response returns a status code specified in
raise_for_status_codes we immediatly raise an error.

If our error is server-side (5xx), it's worth exhausting
retries. If our error is client-side (4xx), it's likely a
problem with our request, and we cut our retries off
immediately and error out.
"""
