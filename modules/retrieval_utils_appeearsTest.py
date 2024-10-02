import requests
import pandas as pd
from tqdm import tqdm

# get site numbers based on state and non-empty params
def get_param_sites(param_codes_str, state_codes, site_types):
    dfs = []
    with tqdm(total=len(state_codes), desc="Processing sites") as pbar:
        for state_code in state_codes:
            url = f"https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={state_code}&parameterCd={param_codes_str}"
            response = requests.get(url)
            
            if response.status_code == 200:
                lines = response.text.splitlines()
                data_lines = [line for line in lines if not line.startswith('#') and line.strip()]
                
                if data_lines:
                    headers = data_lines[0].split('\t')
                    data_lines = [line.split('\t') for line in data_lines[2:]]
                    df = pd.DataFrame(data_lines, columns=headers)
                    if not df.empty:
                        df = df[df['site_tp_cd'].isin(site_types)]
                        dfs.append(df[['site_no', 'station_nm', 'dec_lat_va', 'dec_long_va']])
                    
            pbar.update(1)
        if dfs:
            states_df = pd.concat(dfs, ignore_index=True)
            print(f"{len(states_df)}")
            return states_df
        else:
            print("No sites found for any states")
            return pd.DataFrame()

# point based granule search
def get_site_granules(site_no, station_nm, site_lat, site_lon, temporal_str):
    doi = '10.5067/EMIT/EMITL2ARFL.001'
    cmrurl = 'https://cmr.earthdata.nasa.gov/search/'
    doisearch = cmrurl + 'collections.json?doi=' + doi
    concept_id = requests.get(doisearch).json()['feed']['entry'][0]['id']

    page_num = 1
    page_size = 2000
    granule_arr = []
    
    while True:
        cmr_param = {
            "collection_concept_id": concept_id,
            "page_size": page_size,
            "page_num": page_num,
            "temporal": temporal_str,
            "point": f"{site_lon},{site_lat}"
        }
    
        granulesearch = cmrurl + 'granules.json'
        response = requests.post(granulesearch, data=cmr_param)
        granules = response.json()['feed']['entry']
    
        if granules:
            for g in granules:
                granule_urls = [x['href'] for x in g['links'] if 'https' in x['href'] and '.nc' in x['href'] and '.dmrpp' not in x['href']]
                granule_datetime = g.get('time_start', 'N/A')
                
                granule_arr.append({
                    "site_no": site_no,
                    "station_nm": station_nm,
                    "site_lat": site_lat,
                    "site_lon": site_lon,
                    "granule_urls": granule_urls,
                    "datetime": granule_datetime
                })
                
            page_num += 1
        else:
            if not granule_arr:
                print(f"No granules found for site {site_no}")
            break
    return granule_arr

def get_all_site_granules(site_list, temporal_str):
    all_granules = []
    with tqdm(total=len(site_list), desc="Processing sites") as pbar:
        for i, row in site_list.iterrows():
            site_no = row['site_no']
            station_nm = row['station_nm']
            site_lat = row['dec_lat_va']
            site_lon = row['dec_long_va']   
            site_granules = get_site_granules(site_no, station_nm, site_lat, site_lon, temporal_str)
            all_granules.extend(site_granules)
            pbar.update(1)
        
    return all_granules

# get data within {time window} minutes of granule time
def get_granule_results(site_no, param_cd, scene_time, time_window=30):
    results = []

    start_time = (scene_time - pd.Timedelta(minutes=time_window)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = (scene_time + pd.Timedelta(minutes=time_window)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={site_no}&parameterCd={param_cd}&startDT={start_time}&endDT={end_time}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if 'value' in data and 'timeSeries' in data['value']:
            for ts in data['value']['timeSeries']:
                for value in ts['values'][0]['value']:
                    results.append({
                        'result': value['value'],
                        'result_unit': ts['variable']['unit']['unitCode'],
                        'result_time': value['dateTime']
                    })
    
    return results


# match each granule to temporally closest results
def match_granules(scenes_df, param_codes):
    results = []
    with tqdm(total=len(scenes_df), desc="Processing scenes") as pbar:
        for index, row in scenes_df.iterrows():
            site_no = row['site_no']
            station_nm = row['station_nm']
            lat = row['lat']
            lon = row['lon']
            scene_time = pd.to_datetime(row['datetime'])
            spectra = row['spectra']
            
            closest_result = None 
            closest_time = pd.Timedelta.max
            
            for param_cd in param_codes:
                result_data = get_granule_results(site_no, param_cd, scene_time)
                
                if result_data:
                    for result in result_data:
                        result_time = pd.to_datetime(result['result_time'])
                        time_between = abs(result_time - scene_time)
                        if time_between < closest_time:
                            closest_time = time_between
                            closest_result = {
                                "site_no": site_no,
                                "station_nm": station_nm,
                                "lat": lat,
                                "lon": lon,
                                "datetime": scene_time,
                                "result": result['result'],
                                "result_unit": result['result_unit'],
                                "result_time": result['result_time'],
                                "spectra": spectra
                            }
                if closest_result:
                    results.append(closest_result)
                pbar.update(1)
                
    return pd.DataFrame(results)