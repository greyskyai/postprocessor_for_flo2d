import os
import re
import pandas as pd
from modules.utilities import time_function

# Regular expression patterns
Q_MAX_PATTERN = re.compile(r'MAXIMUM DISCHARGE FROM CROSS SECTION\s+\d+\s+IS:\s+(\d+\.\d+)\s+CFS')
TIME_MAX_PATTERN = re.compile(r'AT TIME:\s+(\d+\.\d+)\s+HOURS')
VOL_PATTERN = re.compile(r'VOLUME OF DISCHARGE IS:\s+(\d+\.\d+)\s+AF')
HYDROGRAPH_PATTERN = re.compile(r'^\s*(\d+\.\d+)', re.MULTILINE)

def read_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()


def extract_max_q_vol_time(file_lines):
    q_max = re.findall(Q_MAX_PATTERN, file_lines)
    time_max = re.findall(TIME_MAX_PATTERN, file_lines)
    vol = re.findall(VOL_PATTERN, file_lines)

    data = {
        'fpxs_id': list(range(1, len(q_max) + 1)),
        'time_max': time_max,
        'q_max': q_max,
        'vol_acft': vol
    }
    return pd.DataFrame.from_dict(data)


def get_start_end_time(file_content):
    hydrograph_times = re.findall(HYDROGRAPH_PATTERN, file_content)

    if hydrograph_times:
        hydrograph_times = [float(t) for t in hydrograph_times]
        return min(hydrograph_times), max(hydrograph_times)
    else:
        pass
        return None, None
    

def extract_max_wse(file_content, start_time, end_time):
    wse_max_values = []
    wse = []

    for line in file_content.split('\n'):
        splt = line.split()

        try:
            if splt and len(splt) > 3 and float(splt[0]) == start_time:
                wse = [float(splt[3])]
            elif splt and len(splt) > 3 and start_time < float(splt[0]) < end_time:
                wse.append(float(splt[3]))
            elif splt and len(splt) > 3 and float(splt[0]) == end_time:
                wse_max_values.append(max(wse))
        except ValueError as e:
            continue

    return wse_max_values


@time_function
def extract_fpxsec_results(file_path):
    file_content = read_file(os.path.join(file_path, 'HYCROSS.OUT'))
    start_time, end_time = get_start_end_time(file_content)
    wse_max_values = extract_max_wse(file_content, start_time, end_time)
    fpxsec_results = extract_max_q_vol_time(file_content)

    if len(wse_max_values) == len(fpxsec_results):
        fpxsec_results['wse_max'] = wse_max_values
    else:
        pass

    return fpxsec_results
