import singer

from .streams import AVAILABLE_STREAMS

LOGGER = singer.get_logger()


def discover(config, state={}):
    LOGGER.info('Starting discovery..')
    data = {}
    data['streams'] = []
    for available_stream in AVAILABLE_STREAMS:
        data['streams'].append(available_stream(config=config, state=state))
    catalog = singer.catalog.Catalog.from_dict(data=data)
    LOGGER.info('Finished discovery..')
    return catalog


def sync(config, catalog, state={}):
    LOGGER.info('Starting sync..')
    selected_streams = {catalog_entry.stream for catalog_entry in catalog.get_selected_streams(state)}

    streams_to_sync = set()
    for available_stream in AVAILABLE_STREAMS:
        if available_stream.stream in selected_streams:
            streams_to_sync.add(available_stream(config=config, state=state))

    for stream in streams_to_sync:
        singer.bookmarks.set_currently_syncing(state=stream.state, tap_stream_id=stream.tap_stream_id)
        stream.write_state()
        stream.write_schema()
        stream.sync()
        singer.bookmarks.set_currently_syncing(state=stream.state, tap_stream_id=None)
        stream.write_state()


def main():
    args = singer.utils.parse_args(required_config_keys=["api_key", "api_guid", "start_date"])
    if args.discover:
        catalog = discover(config=args.config)
        singer.catalog.write_catalog(catalog)
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(args.config)

        sync(config=args.config, catalog=catalog, state=args.state)


if __name__ == "__main__":
    main()
