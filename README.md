# NTRIP RTCM Health Monitor

A standalone Python monitoring and audit tool for NTRIP/RTCM base-station networks.

The tool reads an NTRIP sourcetable, samples mountpoints in parallel, parses RTCM3 MSM messages with `pyrtcm`, collects satellite- and signal-level CNR data, computes peer-relative health metrics, and generates timestamped raw data plus an interactive HTML report.

## Features

- NTRIP sourcetable discovery
- Parallel sampling of multiple mountpoints
- Configurable minimum and maximum sampling duration
- Early stop after a configurable number of MSM epochs
- Country and stream filters with include/exclude patterns
- RTCM3 MSM parsing using `pyrtcm`
- GPS, GLONASS, Galileo, BeiDou, QZSS, SBAS and NavIC support where present
- Satellite-level and signal-level CNR collection
- Country + constellation peer comparison
- 0–100 constellation health score
- Aggregated base health score
- Nearest-neighbor comparison using base coordinates
- Interactive HTML map and charts
- Sortable and filterable HTML summary table
- Timestamped output folders so runs never overwrite each other

## Requirements

- Python 3.10+
- `pyrtcm`

Install the dependency:

```bash
python -m pip install --upgrade pyrtcm
```

## Basic usage

```bash
python ntrip_health_monitor.py \
  --host crtk.net \
  --port 2101 \
  --username YOUR_USERNAME \
  --password YOUR_PASSWORD \
  --workers 5 \
  --min-seconds 5 \
  --max-seconds 20 \
  --min-epochs 3
```

On Windows PowerShell:

```powershell
python.exe .\ntrip_health_monitor.py `
  --host crtk.net `
  --port 2101 `
  --username YOUR_USERNAME `
  --password "YOUR_PASSWORD" `
  --workers 5 `
  --min-seconds 5 `
  --max-seconds 20 `
  --min-epochs 3
```

For credentials, environment variables can also be used:

```text
NTRIP_USERNAME
NTRIP_PASSWORD
```

## Filtering

Country filter:

```bash
--country "HU,SK,!FR"
```

Stream filter:

```bash
--stream "BUD*,PEST*,!TEST*"
```

Rules:

- comma-separated values are OR conditions;
- `!` means exclusion;
- `*` and `?` wildcards are supported.

## Sampling model

Sampling is adaptive.

For example:

```text
--min-seconds 5
--max-seconds 20
--min-epochs 3
```

means:

1. always collect for at least 5 seconds;
2. after that, stop once every observed constellation has at least 3 distinct MSM epochs;
3. never sample a stream for more than 20 seconds.

This keeps large network audits reasonably fast while still collecting a representative MSM sample.

## Output

Every execution creates its own timestamped directory:

```text
runs/
  run_20260818_103500_123/
    sourcetable.csv
    streams.csv
    observations.csv
    summary.csv
    report_data.json
    report.html
```

### `sourcetable.csv`

Raw STR entries discovered from the caster.

### `streams.csv`

One row per sampled NTRIP mountpoint, including connection status, message counts, RTCM message types and source metadata.

### `observations.csv`

Detailed RTCM MSM observations. Typical columns include:

- mountpoint
- timestamp
- constellation
- RTCM message type
- epoch
- satellite PRN
- signal
- CNR / C/N0 in dB-Hz

### `summary.csv`

One row per base × constellation, including:

- satellite count
- CNR mean and median
- CNR p10 / p90
- country satellite median
- satellite delta versus country median
- country CNR median
- CNR delta versus country median
- constellation health score
- overall base health score
- receiver / antenna metadata when discoverable

### `report.html`

Interactive browser report with:

- map of base stations;
- base-health coloring;
- clickable stations;
- constellation radio buttons;
- CNR comparison charts;
- selected station versus four nearest stations;
- country median reference lines;
- satellite-count comparison;
- sortable and filterable summary table.

The report uses Leaflet/OpenStreetMap and Plotly from public CDNs, so Internet access is required for the map and charts when the HTML file is opened.

## Health score

The health score is a **relative diagnostic metric**, not an absolute RTK fix-quality guarantee.

Scores are normalized against the same run's country + constellation peer group.

Current weighting:

| Component | Weight |
|---|---:|
| Satellite-count deviation from peer median | 55% |
| Median CNR deviation from peer median | 40% |
| Sample completeness / transport quality | 5% |

A station at approximately the peer median is designed to score around **80**, rather than 50.

Indicative interpretation:

| Score | Meaning |
|---:|---|
| 90–100 | Excellent / unusually strong |
| 75–89 | Good / normal |
| 60–74 | Fair |
| 40–59 | Suspect |
| 20–39 | Poor |
| 0–19 | Critical |

The overall base health combines the average constellation score with extra weight on the weakest constellation.

These thresholds are intentionally easy to tune after collecting a larger historical dataset.

## Receiver and antenna metadata

The standard NTRIP sourcetable does not define dedicated receiver-model and antenna-model columns.

The monitor therefore performs best-effort extraction from metadata such as `generator`, `misc`, identifier and format details. These fields are displayed for diagnosis but are **not currently used in the health score**.

## Recommended workflow

A practical monitoring workflow is:

1. run the collector periodically;
2. retain timestamped raw outputs;
3. compare health trends over time;
4. correlate low satellite counts with low CNR;
5. compare suspicious bases with geographically nearby bases;
6. only then define operational exclusion rules for a NEAR stream or caster integration.

The longer-term goal is to let a caster consume a compact health-state file and avoid unstable or degraded bases during NEAR selection without coupling the monitoring implementation tightly to the caster itself.

## Release process

Releases are automated with GitHub Actions.

Push a version tag:

```bash
git tag v0.5.0
git push origin v0.5.0
```

The workflow will:

1. check out the tagged revision;
2. validate that the Python file compiles;
3. create a ZIP bundle;
4. generate release notes automatically;
5. create a GitHub Release;
6. upload the Python script and ZIP bundle as release assets.

The workflow file is:

```text
.github/workflows/release.yml
```

## Security

Do not commit NTRIP passwords to the repository.

Use command-line arguments only for local testing, or preferably environment variables/secrets in automated environments.

## Status

This project is currently an engineering/diagnostic tool. The health model should be considered experimental until thresholds have been validated against a sufficiently large set of known-good and known-bad base stations.
