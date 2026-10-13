"""Extract and fingerprint the complete original Kaggle training CSV."""
from pathlib import Path
import csv
import hashlib
import json
import time
import zipfile

ROOT = Path(__file__).resolve().parent
EXPECTED_ROWS = 55_423_856
EXPECTED_BYTES = 5_697_178_298
COLUMNS = ['key', 'fare_amount', 'pickup_datetime', 'pickup_longitude',
           'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'passenger_count']


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    directory = ROOT / 'data'
    directory.mkdir(exist_ok=True)
    destination = directory / 'train.csv'
    archives = list(directory.glob('*.zip'))
    archive_details = None
    if not destination.exists():
        if len(archives) != 1:
            raise FileNotFoundError('Place the Kaggle ZIP or full train.csv in data/.')
        archive = archives[0]
        with zipfile.ZipFile(archive) as package:
            info = package.getinfo('train.csv')
            if info.file_size != EXPECTED_BYTES:
                raise ValueError('The ZIP does not contain the complete original training file.')
            partial = destination.with_suffix('.csv.part')
            with package.open(info) as source, partial.open('wb') as target:
                written = 0
                last_progress = time.monotonic()
                while block := source.read(8 * 1024**2):
                    target.write(block)
                    written += len(block)
                    if time.monotonic() - last_progress > 5:
                        print(f'Extracting: {written / EXPECTED_BYTES:.0%}', flush=True)
                        last_progress = time.monotonic()
            if written != EXPECTED_BYTES:
                raise ValueError('Incomplete extraction')
            partial.replace(destination)
            archive_details = {'file': archive.name, 'bytes': archive.stat().st_size,
                               'sha256': sha256(archive), 'train_crc32': f'{info.CRC:08x}',
                               'train_crc_verified': True}
    if destination.stat().st_size != EXPECTED_BYTES:
        raise ValueError('Unexpected CSV size: the full original file is required.')
    with destination.open(encoding='utf-8', newline='') as source:
        if next(csv.reader(source)) != COLUMNS:
            raise ValueError('Unexpected CSV header')
    digest = hashlib.sha256()
    newlines = 0
    last = b''
    with destination.open('rb') as source:
        while block := source.read(16 * 1024**2):
            digest.update(block)
            newlines += block.count(b'\n')
            last = block[-1:]
    physical_rows = newlines + int(last != b'\n') - 1
    if physical_rows != EXPECTED_ROWS:
        raise ValueError(f'Expected {EXPECTED_ROWS:,} rows; counted {physical_rows:,}.')
    manifest = {'dataset': 'New York City Taxi Fare Prediction',
                'source': 'https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data',
                'license': 'Subject to competition rules; educational use permitted, redistribution restricted.',
                'file': 'data/train.csv', 'csv_bytes': destination.stat().st_size,
                'csv_sha256': digest.hexdigest(), 'expected_rows': EXPECTED_ROWS,
                'physical_rows': physical_rows, 'columns': COLUMNS,
                'row_count_note': 'Header excluded. Logical rows independently checked by Pandas and Dask in the benchmark.',
                'archive': archive_details, 'original_csv_unchanged': True,
                'verified_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
    existing = directory / 'manifest.json'
    if archive_details is None and existing.exists():
        manifest['archive'] = json.loads(existing.read_text())['archive']
    existing.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    print(f'VERIFIED: {physical_rows:,} rows; {destination.stat().st_size:,} bytes', flush=True)


if __name__ == '__main__':
    main()
