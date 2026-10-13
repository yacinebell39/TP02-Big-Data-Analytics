# Original Kaggle dataset

Download the full training CSV or competition ZIP from:
https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data

Use your own Kaggle account and accept the competition rules. Put the file here and run `prepare.py` from the project root. The script extracts only `train.csv`; the test set, submission template and coupons document are outside this TP's scope.

Expected input: 55,423,856 data rows, eight columns and 5,697,178,298 bytes. The CSV is not edited. `manifest.json` records its real SHA-256, row count and archive CRC. Benchmark parsers independently confirm the logical row count.

No prediction model or Kaggle competition submission is required for TP02. Statistics include all raw rows, including negative fares and outliers. Missing values are excluded from each metric's valid count when computing means.

Raw CSV/ZIP files are ignored by Git because competition rules restrict their redistribution. The manifest and download instructions are included in the submission.
