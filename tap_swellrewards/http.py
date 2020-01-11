import requests
import singer
from singer import metrics
import backoff

LOGGER = singer.get_logger()

BASE_URL = "https://app.swellrewards.com/api/v2"

class RateLimitException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.api_key = config.get("api_key")
        self.api_secret = config.get("api_guid")
        self.session = requests.Session()
        self.base_url = BASE_URL

    def prepare_and_send(self, request):
        request.headers["x-api-key"] = self.api_key
        request.headers["x-guid"] = self.api_secret
        return self.session.send(request.prepare())

    def url(self, version, raw_path):
        return _join(self.base_url, raw_path)

    def create_get_request(self, version, path, **kwargs):
        return requests.Request(method="GET",
                                url=self.url(version, path),
                                **kwargs)

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 503, 504]:
            raise RateLimitException()
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def GET(self, version, request_kwargs, *args, **kwargs):
        req = self.create_get_request(version, **request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)
