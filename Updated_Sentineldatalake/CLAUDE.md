# AC&CD — Active C&C Detector

Detects active C2 beaconing by analysing time delta distribution and data size distribution of network traffic between the same source-destination pairs. Adapted from [Cyb3r-Monk/ACCD](https://github.com/Cyb3r-Monk/ACCD) — original pandas implementation ported to PySpark for the Microsoft Sentinel data lake.

## Notebooks

| File | Purpose |
|---|---|
| `Updated_Sentineldatalake/ACCD_SentinelLake.ipynb` | PySpark port — runs on Sentinel data lake |
| `AC&CD.ipynb` | Original pandas version — runs locally against CSV sample data |

## Execution environment

The Sentinel notebook runs on the **Microsoft Sentinel VS Code extension kernel** — it connects to a managed Spark cluster in the Sentinel data lake workspace, not a local Python kernel. The `spark` SparkSession and `MicrosoftSentinelProvider` are injected by the extension at runtime. Do not run it in a standard Jupyter environment without adapting the data loading cell.

PySpark version on the cluster matters: `F.array_compact()` (Cell 4) requires Spark 3.4+. On older clusters, replace it with `F.expr("filter(deltas, x -> x is not null)")`.

## Algorithm

1. Load `CommonSecurityLog` (CEF proxy/firewall logs) for the lookback window
2. Compute per-connection time deltas using a Window + `lag()` — stays distributed
3. Group by `(SourceIP, DestinationHostName, RequestMethod)`, collect delta and byte arrays
4. Filter on minimum connection count, then calculate 15th/30th/45th percentile and MAD statistics on time deltas and sent bytes
5. Apply interactivity filters (active-phase duration ≥ 1h, ≥ 24 connections)
6. Apply execution bytes filter (at least 10 connections with received bytes > 20KB)
7. Score each pair 0–1 on time consistency (`tsScore`) and data size consistency (`dsScore`); final `Score` = average
8. Enrich with destination prevalence and filter to `Score > 0.85`, prevalence < 5

The key insight: rather than detecting fixed-interval beaconing, ACCD targets the **active keyboard phase** — the window where an operator is issuing commands — which produces tight clustering in the lower percentiles regardless of jitter.

## Threshold tuning

All thresholds live in **Cell 2 (Configuration)**. Production defaults are aggressive and will return zero results in low-volume environments.

| Variable | Default | When to lower |
|---|---|---|
| `MIN_CONN_COUNT` | 24 | Test envs with sparse traffic; try 10–15 |
| `MIN_INTERACTIVITY_CONN` | 24 | Same; match `MIN_CONN_COUNT` |
| `MAX_PREVALENCE` | 5 | Noisy envs where many hosts share destinations; try 10–20 |
| `MIN_INTERACTIVITY_DURATION` | 3600s | Short test sessions; try 600 |
| `LOOKBACK_DAYS` | 7 | Reduce to 1–2 for faster iteration during testing |
| `MIN_SCORE` | 0.85 | Lower to 0.7 to see near-miss candidates |

The jitter thresholds (`JITTER_THRESHOLD_TS = 55`, `JITTER_THRESHOLD_DS = 25`) are config variables but are not currently wired into the scoring UDFs — the UDF default parameters shadow them. To make changes take effect, pass them explicitly via `F.lit()` when calling the UDFs.

## Testing requirements

- The grouping key is `(SourceIP, DestinationHostName, RequestMethod)` — **single-occurrence pairs cannot be scored**. Test data must contain repeated connections between the same source and destination.
- Minimum viable test pair: 24+ connections with consistent timing (≥ `MIN_CONN_COUNT`), spanning at least 1 hour, with at least 10 connections returning > 20KB.
- The sample data in `sample-data/sample_beacon_dataset.csv` covers the original pandas notebook only and uses a different timestamp format — it is not directly usable with the PySpark notebook.

## Known issues

- **Jitter thresholds not wired to config** (Cell 9): `udf_ts_score` and `udf_ds_score` use Python default parameters, so `JITTER_THRESHOLD_TS`/`JITTER_THRESHOLD_DS` from Cell 2 have no effect unless passed via `F.lit()`.
- **`MIN_EXECUTION_BYTES` hardcoded in UDF** (Cell 9): `udf_ds_score` checks `< 20000` directly instead of reading the config variable.
- **Time delta precision**: The original pandas notebook uses `dt.seconds` (seconds component only — a 65s gap is recorded as 5s). The PySpark version uses `unix_timestamp()` subtraction (total seconds — correct). Results will differ for any delta > 59s.

## Table schema

Targets `CommonSecurityLog` (CEF). To use ASIM-normalised tables, swap to `ASimNetworkSessionLogs` and update the column mapping variables in Cell 2 (e.g. `SrcIpAddr` instead of `SourceIP`).
