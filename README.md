# tap-swellrewards

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Swell Rewards](https://loyaltyapi.yotpo.com/reference)
- Extracts the following resources:
  - [customers](https://loyaltyapi.yotpo.com/reference#customers)
- Outputs the schema for each resource

## Quick Start

1. Install

    ```bash
    $ pip install tap-swellrewards
    ```

2. Get API keys

    You can find your `api_key` and `GUID` in your Swell Rewards settings.


3. Create the config file

   You must create a JSON configuration file that looks like this:

   ```json
   {
       "last_seen_at": "2015-01-01",
       "api_key": "...",
       "api_guid": "..."
   }
   ```

   The `last_seen_at` parameter determines the starting date for the last registered
   customer activity within the Yotpo loyalty and rewards system e.g. purchase, redemption, etc. 
   For example, pass a value of `2019-01-01` to fetch active customers since January 1st, 2019.

4. Run the Tap in Discovery Mode

    ```bash
    $ tap-swellrewards -c config.json -d
    ```

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

5. Run the Tap in Sync Mode

    ```bash
    $ tap-swellrewards -c config.json -p catalog-file.json
    ```

---

Copyright &copy; 2020 Stitch
