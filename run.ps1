$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$tpPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $tpPython)) { throw 'Create .venv and install requirements.txt first; see README.md.' }
& $tpPython -B prepare.py
if ($LASTEXITCODE -ne 0) { throw 'Dataset preparation failed.' }
& $tpPython -B -m unittest test_analysis -v
if ($LASTEXITCODE -ne 0) { throw 'Aggregation checks failed.' }
& $tpPython -B benchmark.py --repeats 3 --budget-mib 8192 --threads 4 --block-mib 16
if ($LASTEXITCODE -ne 0) { throw 'Full benchmark failed; inspect results.' }
& $tpPython -B report.py
if ($LASTEXITCODE -ne 0) { throw 'Report generation failed.' }
Write-Output 'TP completed: report\TP02_Taxi_Fares.pdf'
