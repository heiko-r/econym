# Econym

A minimal, fast reverse geocoder, returning the name of the nearest populated place for any coordinate.

As long as you need nothing but a name near the given coordinates, and don't need perfection, this will return a name within about 2 ms plus network latency.

## License

This application is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

The information used is derived from the [Overture Maps Divisions theme](https://docs.overturemaps.org/guides/divisions/), which is made available under the [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/):
- © OpenStreetMap contributors. Available under the [Open Database License](https://www.openstreetmap.org/copyright).
- [geoBoundaries](https://www.geoboundaries.org/). Available under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- [Esri Community Maps contributors](https://communitymaps.arcgis.com/home/). Available under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- [Land Information New Zealand (LINZ)](https://www.linz.govt.nz/). Available under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

## Usage

### Preparing the data

1. Download the global Overture Maps Divisions theme data (~650 MB), e.g.:
   ```bash
   aws s3 cp --no-sign-request --recursive s3://overturemaps-us-west-2/release/2026-04-15.0/theme=divisions/type=division/ ./
   ```
2. Extract the relevant data into a local `geocoder.rkyv` file (~310 MB, about 2 minutes):
   ```bash
   econym load --input path/to/overture.parquet
   ```
3. Test:
   ```bash
   econym lookup --lat 52.522 --lon 13.325
   ```

### Run the server

```bash
econym serve
```

### Test the server

```bash
curl "http://localhost:3000/lookup?lat=52.522&lon=13.325"
```

### In-memory mode

The `lookup` and `serve` commands support an `--in-memory` flag to load the geocoder data into memory instead of using the file system. This will result in faster lookups, but will use significantly more memory (~500 MB instead of ~45 MB).
