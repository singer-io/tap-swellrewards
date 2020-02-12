import inspect
import os
import time
from datetime import datetime, timedelta
from typing import Dict

import requests
import singer

LOGGER = singer.get_logger()


class SwellRewardsStream:
    BASE_URL = "https://app.swellrewards.com/api/v2"

    def __init__(self, config, state):
        self.config = config
        self.api_key = config.get('api_key')
        self.api_guid = config.get('api_guid')
        self.state = state
        self.params = {
            "per_page": config.get('per_page', 100),
            "page": 1,
            "last_seen_at": config.get('start_date')
        }
        self.schema = self.load_schema()
        self.metadata = singer.metadata.get_standard_metadata(schema=self.load_schema(),
                                                              key_properties=self.key_properties,
                                                              valid_replication_keys=self.valid_replication_keys,
                                                              replication_method=self.replication_method)

        config_stream_params = config.get('streams', {}).get(self.tap_stream_id)

        if config_stream_params is not None:
            for key in config_stream_params.keys():
                if key not in self.valid_params:
                    raise RuntimeError(f"/{self.tap_stream_id} endpoint does not support '{key}' parameter.")

            self.params.update(config_stream_params)

        for param in self.required_params:
            if param not in self.params.keys():
                if param == 'until':
                    self.params.update({"until": datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%SZ')})
                else:
                    raise RuntimeError(f"Parameter '{param}' required but not supplied for /{self.tap_stream_id} endpoint.")

    def get(self, key: str):
        '''Custom get method so that Singer can
        access Class attributes using dict syntax.
        '''
        return inspect.getattr_static(self, key, default=None)

    def _get_abs_path(self, path: str) -> str:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self) -> Dict:
        '''Loads a JSON schema file for a given
        SwellRewards resource into a dict representation.
        '''
        schema_path = self._get_abs_path("schemas")
        return singer.utils.load_json(f"{schema_path}/{self.tap_stream_id}.json")

    def write_schema(self):
        '''Writes a Singer schema message.'''
        return singer.write_schema(stream_name=self.stream, schema=self.schema, key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    def _construct_headers(self) -> Dict:
        headers = requests.utils.default_headers()
        headers["User-Agent"] = "python-swellrewards-tap"
        headers["x-guid"] = self.api_guid
        headers["x-api-key"] = self.api_key
        headers["Content-Type"] = "application/json"
        return headers

    def _get(self, url_suffix: str, params: Dict = None) -> Dict:
        url = self.BASE_URL + url_suffix
        headers = self._construct_headers()
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            LOGGER.warn("Rate limit reached. Trying again in 60 seconds.")
            time.sleep(60)
            response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def update_bookmark(self, bookmark, value):
        if bookmark is None:
            new_bookmark = value
        else:
            new_bookmark = max(bookmark, value)
        return new_bookmark

    def _list_resource(self, url_suffix: str, params: Dict = None):
        response = self._get(url_suffix=url_suffix, params=params)
        return SwellRewardsResponse(self, url_suffix, params, response)


class SwellRewardsResponse:
    def __init__(self, client, url_suffix, params, response):
        self.client = client
        self.url_suffix = url_suffix
        self.params = params
        self.response = response

    def __iter__(self):
        self._iteration = 0
        return self

    def __next__(self):
        self._iteration += 1
        if self._iteration == 1:
            return self

        if self.response.get("links") is None:
            raise StopIteration

        if self.response.get("links")["total_pages"] > self.params["page"]:
            self.params["page"] += 1
            self.response = self.client._get(
                url_suffix=self.url_suffix, params=self.params
            )
            return self
        else:
            raise StopIteration

    def get(self, key, default=None):
        return self.response.get(key, default)


class CustomersStream(SwellRewardsStream):
    tap_stream_id = 'customers'
    stream = 'customers'
    key_properties = ['email']
    valid_replication_keys = []
    replication_method = 'FULL_TABLE'
    valid_params = [
        'page',
        'per_page',
        'last_seen_at',
    ]
    required_params = ['last_seen_at']

    def __init__(self, config, state, **kwargs):
        super().__init__(config, state)

    def sync(self):
        record_metadata = singer.metadata.to_map(self.metadata)

        with singer.metrics.job_timer(job_type=f"list_{self.tap_stream_id}"), \
          singer.metrics.record_counter(endpoint=self.tap_stream_id) as counter, \
          singer.Transformer() as transformer:
            for page in self._list_resource(url_suffix="/customers/all", params=self.params):
                for record in page.get(self.tap_stream_id):
                  transformed_record = transformer.transform(data=record, schema=self.schema, metadata=record_metadata)
                  singer.write_record(stream_name=self.stream, time_extracted=singer.utils.now(), record=transformed_record)
                  counter.increment()


AVAILABLE_STREAMS = {
    CustomersStream
}
