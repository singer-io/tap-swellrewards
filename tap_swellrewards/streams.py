import singer
from singer import metrics, transform
import pendulum

LOGGER = singer.get_logger()

PAGE_SIZE = 100


class Stream(object):
    def __init__(self, tap_stream_id, pk_fields, path,
                 returns_collection=True,
                 collection_key=None,
                 pluck_results=False,
                 custom_formatter=None,
                 version=None):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        self.path = path
        self.returns_collection = returns_collection
        self.collection_key = collection_key
        self.pluck_results = pluck_results
        self.custom_formatter = custom_formatter or (lambda x: x)
        self.version = version

        self.start_date = None

    def get_start_date(self, ctx, key):
        if not self.start_date:
            self.start_date = ctx.get_bookmark([self.tap_stream_id, key])
        return self.start_date

    def metrics(self, records):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(records))

    def write_records(self, records):
        singer.write_records(self.tap_stream_id, records)
        self.metrics(records)

    def format_response(self, response):
        if self.pluck_results:
            response = response['response']

        if self.returns_collection:
            if self.collection_key:
                records = (response or {}).get(self.collection_key, [])
            else:
                records = response or []
        else:
            records = [] if not response else [response]
        return self.custom_formatter(records)


class Paginated(Stream):
    def get_params(self, ctx, page):
        return {
            "per_page": PAGE_SIZE,
            "page": page
        }

    def on_batch_complete(self, ctx, records, product_id=None):
        self.write_records(records)
        return True

    def _sync(self, ctx, path=None, product_id=None):
        if path is None:
            path = self.path

        if product_id:
            bookmark_name = 'product_{}.since_date'.format(product_id)
        else:
            bookmark_name = 'since_date'
        ctx.update_start_date_bookmark([self.tap_stream_id, bookmark_name])

        schema = ctx.catalog.get_stream(self.tap_stream_id).schema.to_dict()

        page = 1
        while True:
            params = self.get_params(ctx, page)
            opts = {"path": path, "params": params}
            resp = ctx.client.GET(self.version, opts, self.tap_stream_id)
            raw_records = self.format_response(resp)
            records = [transform(record, schema) for record in raw_records]

            if not self.on_batch_complete(ctx, records, product_id):
                break

            if len(records) == 0:
                break
            page += 1

    def sync(self, ctx):
        self._sync(ctx)

    def _transform_dt(self, time_str):
        return pendulum.parse(time_str).in_timezone("UTC")

    def update_bookmark(self, ctx, max_record_ts, path_key):
        path = [self.tap_stream_id, path_key]
        bookmark_ts = self._transform_dt(ctx.get_bookmark(path))

        last_record_ts = self._transform_dt(max_record_ts)

        if last_record_ts > bookmark_ts:
            ctx.set_bookmark(path, last_record_ts.to_date_string())


class Customers(Paginated):
    def get_params(self, ctx, page):
        last_seen_at_raw = self.get_start_date(ctx, 'last_seen_at')
        last_seen_at = pendulum.parse(last_seen_at_raw).in_timezone("UTC")

        return {
            "per_page": PAGE_SIZE,
            "page": page,
            "last_seen_at": last_seen_at.to_iso8601_string(),
        }

    def on_batch_complete(self, ctx, records, product_id=None):
        self.write_records(records)

        if len(records) == 0:
            return False

        last_record = records[-1]
        max_record_ts = last_record['last_seen_at']
        self.update_bookmark(ctx, max_record_ts, 'since_date')

        return True

all_streams = [

    Customers(
        "customers",
        ["email"],
        "customers/all",
        collection_key="customers"
    )
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
