# smartmet-engine-observation

Part of [SmartMet Server](https://github.com/fmidev/smartmet-server). See the [SmartMet Server documentation](https://github.com/fmidev/smartmet-server) for a full overview of the ecosystem.

## Overview

The observation engine provides access to weather station observation data for SmartMet Server. It supports multiple database backends and caches observation data for efficient querying by plugins.

## Features

- Weather station observation data retrieval
- Multiple backend support (Oracle via [smartmet-library-delfoi](https://github.com/fmidev/smartmet-library-delfoi), PostgreSQL)
- In-memory observation cache for high-throughput access
- Used by [smartmet-plugin-timeseries](https://github.com/fmidev/smartmet-plugin-timeseries) and [smartmet-plugin-wfs](https://github.com/fmidev/smartmet-plugin-wfs)

## License

MIT — see [LICENSE](LICENSE)

## Contributing

Bug reports and pull requests are welcome on [GitHub](../../issues).
