# Valhalla Overture Parser

Out-of-tree Valhalla plugin that reads Overture Maps GeoParquet instead of OSM PBF to build routing tiles.

Valhalla's pipeline: `PBF parser -> OSMData -> GraphBuilder -> tiles`. The first step is replaced with a Parquet reader. Everything downstream is unchanged.

## Quick start

```bash
make build        # build the tile builder
make tiles        # build tiles from Copenhagen test data
make test         # start valhalla, run pytest, tear down
make up           # start routing service on :8002
make down
```

## Overture to Valhalla mapping

| Overture | Valhalla | Notes |
|----------|----------|-------|
| `connector.id` | `OSMNode.node_id` | FNV-1a hash to uint64 |
| `connector.geometry` | `OSMNode.latlng` | WKB point |
| `segment.id` | `OSMWay.way_id` | FNV-1a hash to uint64 |
| `segment.class` | `OSMWay.use`, `road_class` | see table below |
| `segment.road_surface[0].value` | `OSMWay.surface` | surface enum |
| `segment.names.primary` | `UniqueNames` + `OSMWay.name_index` | string table |
| `segment.connectors[].connector_id` | way node references | matched via `at` fraction |
| `segment.speed_limits[0].max_speed` | `OSMWay.speed_limit` | KPH |
| `segment.geometry` | edge shape | WKB LineString |

### Road classes

| Overture | Use | RoadClass |
|----------|-----|-----------|
| motorway | kRoad | kMotorway |
| trunk | kRoad | kTrunk |
| primary | kRoad | kPrimary |
| secondary | kRoad | kSecondary |
| tertiary | kRoad | kTertiary |
| residential | kRoad | kResidential |
| living_street | kLivingStreet | kResidential |
| service | kServiceRoad | kServiceOther |
| footway | kFootway | kServiceOther |
| pedestrian | kPedestrian | kServiceOther |
| cycleway | kCycleway | kServiceOther |
| path | kPath | kServiceOther |
| steps | kSteps | kServiceOther |
| track | kTrack | kServiceOther |

## Pipeline

Parse Overture Parquet, write `ways.bin` + `way_nodes.bin`, then run the standard Valhalla pipeline: ConstructEdges, Build, Enhance, Hierarchy, Shortcuts, Validate (spatial binning).

`--pedestrian-only` skips motorway/trunk segments for ~60-70% smaller tiles.

## Test data

Copenhagen: 113K connectors, 75K segments. Builds to 112K nodes, 274K directed edges, 4 tiles (~21MB) in ~4 seconds.

```bash
make download-data   # download fresh Overture data for Copenhagen
```

## Project structure

```
src/
  overture_parser.h    # parser interface
  overture_parser.cc   # parser implementation
  main.cc              # pipeline orchestration
tests/
  test_route.py        # pytest routing tests
  conftest.py          # pytest-docker fixture
data/
  connectors.parquet   # Copenhagen connectors
  segments.parquet     # Copenhagen segments
  valhalla.json        # valhalla config
docker-compose.yml     # valhalla service for tests
CMakeLists.txt
Dockerfile
```

## Deployment

Hetzner via Kamal. See `infra/` and `config/deploy.yml`.

```bash
make infra    # provision server
make deploy   # deploy routing service
make logs
```
