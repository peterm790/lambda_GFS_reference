import fsspec
import s3fs
import json
import cfgrib
import zarr
import numpy as np
from kerchunk.grib2 import scan_grib
from kerchunk.combine import merge_vars, MultiZarrToZarr
import datetime

def handler(event, context):
    try: # if triggered via cron to consolidate there will be a run description in the event
        run = event['run']
        date = datetime.date.today().strftime('%Y%m%d')
        fs = fsspec.filesystem('s3')
        print(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/{date}/{run}/")
        files = fs.ls(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/{date}/{run}/")
        files = ['s3://'+f for f in files]
        print(len(files))
        ref = MultiZarrToZarr(files,concat_dims = 'valid_time', remote_protocol='s3')
        with fs.open(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/latest.json") as f:
            f.write(json.dumps(ref.translate()).encode())
    except:
        object_key = event["Records"][0]["s3"]["object"]["key"]
        fs = fsspec.filesystem('s3')
        with fs.open(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/{object_key}", 'rb') as f:
            input_data = json.load(f)
        file_url = input_data['grib_file_url']
        afilter = input_data['filter']
        outfile = input_data['reference_file_name']
        so = dict(anon=True, default_fill_cache=False, default_cache_type='first')
        out = scan_grib(file_url, storage_options = so, filter = afilter)
        ref = merge_vars(out)
        with fs.open(f"s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/{outfile}", 'wb') as f:
            f.write(json.dumps(ref).encode())

