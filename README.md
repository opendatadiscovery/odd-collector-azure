[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)
[![forthebadge](https://forthebadge.com/images/badges/for-you.svg)](https://forthebadge.com)
# odd-collector-azure
ODD Collector is a lightweight service which gathers metadata from all your data sources.

To learn more about collector types and ODD Platform's architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Preview
 - [Implemented adapters](#implemented-adapters)
 - [How to build](#building)
 - [Docker compose example](#docker-compose-example)

## Implemented adapters
| Service | Config example                          |
|---------|-----------------------------------------|
| PowerBi | [config](config_examples/power_bi.yaml) |



## Building
```bash
docker build .
```

## Docker compose example
Due to the Plugin is inherited from `pydantic.BaseSetting`, each field missed in `collector-config.yaml` can be taken from env variables.

Custom `.env` file for docker-compose.yaml
```
CLIENT_ID=
CLIENT_SECRET=
USERNAME=
PASSWORD=
DOMAIN=
PLATFORM_HOST_URL=http://localhost:8080
```

Custom `collector-config.yaml`
```yaml
platform_host_url: "http://localhost:8080"
default_pulling_interval: 10
token: ""
plugins:
  - type: powerbi
    name: powerbi_adapter
    client_id: <client_id of registered in AD app>
    client_secret: <client secret of registered in AD app>
    username: <email>
    password: password
    domain: yourmomain.com
```

docker-compose.yaml
```yaml
version: "3.8"
services:
  # --- ODD Platform ---
  database:
    ...

  odd-platform:
    ...
  
  odd-collector-azure:
    image: 'ghcr.io/opendatadiscovery/odd-collector-azure:latest'
    restart: always
    volumes:
      - collector_config.yaml:/app/collector_config.yaml
    environment:
      - CLIENT_ID=${CLIENT_ID}
      - CLIENT_SECRET=${CLIENT_SECRET}
      - USERNAME=${USERNAME}
      - PASSWORD=${PASSWORD}
      - DOMAIN=${DOMAIN}
      - PLATFORM_HOST_URL=${PLATFORM_HOST_URL}
      - LOGLEVEL='DEBUG'
    depends_on:
      - odd-platform
```
