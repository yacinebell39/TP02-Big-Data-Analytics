# TP02 - Big Data Analytics: Taxi Fares Beyond RAM

University of Eloued - Second-year Master IA & Data Science - 2026/2027.

This lab compares full Pandas loading, incremental Pandas processing and lazy parallel Dask computation on the complete **New York City Taxi Fare Prediction** training dataset. It includes the source code, measured results, memory traces, correctness tests and report.

**[Read the report](report/TP02_Taxi_Fares.pdf)** · **[Benchmark results](results/benchmark.csv)** · **[Validation](results/validation.json)**

## Dataset

- Source: [Kaggle - New York City Taxi Fare Prediction](https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data).
- Input: the original `train.csv`, **55,423,856 rows and 8 columns**.
- CSV size: **5,697,178,298 bytes**, or 5.697 GB / 5.306 GiB.
- Complete input, without sampling, filtering, duplication or synthetic inflation.
- SHA-256, archive CRC and verified row count: [data/manifest.json](data/manifest.json).

Download the full CSV or competition ZIP through your own Kaggle account and place it in `data/`. Competition rules permit educational use and restrict redistribution of raw data; the CSV and ZIP are excluded from this repository.

## Methods and measured results

Each method ran three times in a fresh process. All workers used the same 2,048 MiB Windows committed-memory budget. Process-tree resident memory was sampled with psutil every 100 ms.

| Method | Outcome | Median processing time | Median peak sampled RSS |
| --- | --- | --- | --- |
| Full Pandas loading | MemoryError in all three attempts | 5.42 s until failure | 1,582.8 MiB |
| Pandas, 100,000-row chunks | All three completed | 65.45 s | 154.1 MiB |
| Dask, 4 threads / 16 MiB blocks | All three completed | 160.20 s | 451.7 MiB |

The controlled OOM challenge uses `pd.read_csv(..., engine='python')` after preliminary C-parser full-load attempts exited natively under the memory cap. It demonstrates a real allocation failure within a 2 GiB worker budget; it does not claim to exhaust the machine's 31.85 GiB of physical RAM. Chunking and Dask both use the C parser and the same column types. Full-load duration is time until failure; completed processing times are compared between chunking and Dask.

Dask used 339 partitions. Explicit optimization reduced the union graph from 3,493 to 1,602 distinct tasks. Dask was slower than chunking in these measurements; the report discusses scheduling, parsing and query-sharing overhead. The OS file cache was not flushed, and sampled RSS can miss brief peaks.

![Measured performance](figures/performance.png)

## Aggregation and correctness

The methods calculate all column non-null counts, sums and means for six numeric columns, and fare totals/counts/means grouped by passenger count. Chunking accumulates sums and valid counts with compensated summation; Dask uses native lazy reductions.

All six successful full-data runs matched, with exact integer counts and floating-point tolerances `rtol=1e-10`, `atol=1e-6`. Mean raw fare was **11.34504560 USD**. Two correctness tests cover known answers, missing values, uneven final chunks and multiple Dask partitions.

## Run on Windows

Place the downloaded competition ZIP or full `train.csv` inside `data/`, then run:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -B prepare.py
.\.venv\Scripts\python.exe -B -m unittest test_analysis -v
.\.venv\Scripts\python.exe -B benchmark.py --repeats 3 --budget-mib 2048 --threads 4 --block-mib 16
.\.venv\Scripts\python.exe -B report.py
```

After setup, `powershell -ExecutionPolicy Bypass -File run.ps1` runs preparation, tests, benchmarks and report generation. Reserve about 10 GB of disk space. No GPU or database is required. `requirements.lock.txt` records the installed environment.

The memory budget uses Windows Job Objects. `--budget-mib 0` disables it, including on other operating systems, and changes the experimental conditions. `report.py` reads the recorded results in `results/`.

## Project files

| File / folder | Purpose |
| --- | --- |
| `analysis.py` | Full loading, streaming accumulator and lazy Dask reductions |
| `benchmark.py` | Isolated workers, memory cap, psutil profiling and validation |
| `prepare.py` | Original CSV extraction and integrity checks |
| `test_analysis.py` | Known-answer and partition/chunk-boundary tests |
| `run.ps1` | Complete local rerun |
| `report.py` | Regenerate the report and figures from measured results |
| `data/manifest.json` | Dataset source, size, checksum and row count |
| `results/` | Final trials, exceptions, memory samples and verification |
| `results/pilot_*/` | Preliminary diagnostics, excluded from the final comparison |
| `figures/` | Performance, memory evolution and execution-structure charts |
| `report/` | PDF and Markdown report |

## References

- [Dataset and competition rules](https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/rules).
- [Pandas read_csv](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html).
- [Dask DataFrame design](https://docs.dask.org/en/stable/dataframe-design.html).
- [Dask optimizer](https://docs.dask.org/en/stable/dataframe-optimizer.html).
- [Windows Job Objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects).
