import fsspec
import json
import datetime


def handler(event, context):
    fs = fsspec.filesystem('s3', anon=True)
    date = datetime.date.today().strftime('%Y%m%d')
    latest_run = fs.ls(f's3://noaa-gfs-bdp-pds/gfs.{date}/')[-1].split('/')[-1]
    files = fs.glob(f's3://noaa-gfs-bdp-pds/gfs.{date}/{latest_run}/atmos/gfs.t{latest_run}z.pgrb2.0p25.f*')
    files = [f for f in files if f.split('.')[-1] != 'idx']
    files = sorted(['s3://'+f for f in files])
    files_first_5 = files[:121:1] # 03/01/2025 now do all files not just 3 hourly (5 days hourly)
    files_next_10 = files[121:]
    files = files_first_5+files_next_10
    fs_ = fsspec.filesystem('s3',skip_instance_cache = True)
    old_triggers = fs_.ls('s3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/triggers/')
    old_triggers = sorted(['s3://'+f for f in old_triggers])
    try:
        fs_.rm(old_triggers)
    except:
        pass
    for file in files:
        name = f"{date}_{file.split('/')[-1]}"
        trigger = {}
        trigger['grib_file_url'] = file
        trigger['filter'] = {'typeOfLevel': 'heightAboveGround'}
        trigger['reference_file_name'] = f"{date}/{latest_run}/{name}_reference.json"
        with fs_.open(f's3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/triggers/{name}_trigger.json', 'w') as file:
            json.dump(trigger, file)