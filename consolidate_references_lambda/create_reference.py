import fsspec
import s3fs
import json
import cfgrib
import zarr
from kerchunk.combine import MultiZarrToZarr
import datetime

def handler(event, context):
    run = event['run']
    date = datetime.date.today().strftime('%Y%m%d')
    if not int(event['offset']) == 0:
        date = (datetime.date.today() - datetime.timedelta(days=int(event['offset']))).strftime('%Y%m%d')
    fs = fsspec.filesystem('s3')
    print(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/{date}/{run}/")
    files = fs.ls(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/{date}/{run}/")
    files = ['s3://'+f for f in files]
    print(len(files))
    ref = MultiZarrToZarr(files,concat_dims = 'valid_time', remote_protocol='s3')
    with fs.open(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/latest.json") as f:
        f.write(json.dumps(ref.translate()).encode())

