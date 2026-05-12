<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# HunabKu DRI UdeA plugin

HunabKu plugin for the International Affairs Office of Universidad de Antioquia.

# Description

This plugin exposes DRI UdeA agreements and mobility records from the `international` MongoDB database through HunabKu HTTP endpoints.

Both endpoints return only the content stored in each record's `raw_payload` field. The fields `N`, `D:` and `Observaciones` are excluded from the response. The fields `vía`/`via` and `ámbito`/`ambito` are normalized as `VÍA` and `ÁMBITO`.

# Package

```sh
pip install hunabku_dri_udea
```

# Endpoints

## Agreements

Returns agreement records filtered by scope.

```text
GET /agreements
```

Parameters:

- `apikey`: required API key.
- `ambito`: optional scope. Accepted values are `nacional` and `internacional`. If omitted, all records are returned.
- `ámbito`: accepted alias for `ambito`.
- `format`: optional response format. Accepted values are `json` and `csv`. Default is `json`.

Examples:

```sh
curl "http://apis.colav.co:8000/agreements?apikey=XXX"
curl "http://apis.colav.co:8000/agreements?apikey=XXX&ambito=nacional"
curl "http://apis.colav.co:8000/agreements?apikey=XXX&ambito=internacional&format=csv"
```

## Mobility

Returns mobility records filtered by scope.

```text
GET /mobility
```

Parameters:

- `apikey`: required API key.
- `ambito`: optional scope. Accepted values are `nacional` and `internacional`. If omitted, all records are returned.
- `ámbito`: accepted alias for `ambito`.
- `format`: optional response format. Accepted values are `json` and `csv`. Default is `json`.

Examples:

```sh
curl "http://apis.colav.co:8000/mobility?apikey=XXX"
curl "http://apis.colav.co:8000/mobility?apikey=XXX&ambito=nacional"
curl "http://apis.colav.co:8000/mobility?apikey=XXX&ambito=internacional&format=csv"
```

# Configuration

Default configuration values:

- `db_uri`: `mongodb://localhost:27017/`
- `db_name`: `international`
- `mobility_collection_name`: `mobilities`
- `agreements_collection_name`: `agreements_raw`
- `apikey`: `colav`

# License

BSD-3-Clause License

# Links

http://colav.udea.edu.co/
