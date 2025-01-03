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
    files_hourly = files[:121:1]
    files_3_hourly = files[:121:3] + files[121:]
    # 3 hourly
    ref = MultiZarrToZarr(files_3_hourly,concat_dims = 'valid_time', remote_protocol='s3').translate()
    with fs.open(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/latest.json", 'wb') as f:
        f.write(json.dumps(ref).encode())
    with fs.open(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/{date}_{run}_3_hourly.json", 'wb') as f:
        f.write(json.dumps(ref).encode())
    # hourly
    ref = MultiZarrToZarr(files_hourly,concat_dims = 'valid_time', remote_protocol='s3').translate()
    with fs.open(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/latest_hourly.json", 'wb') as f:
        f.write(json.dumps(ref).encode())
    with fs.open(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/{date}_{run}_1_hourly.json", 'wb') as f:
        f.write(json.dumps(ref).encode())