import pm4py
import sys
import os

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.abspath(''))
# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)
sys.path.append(parent)

from pm4py.objects.log.obj import EventLog
from bpmi40.WM_util import case_time
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

def time_based_split(log, save_path, process_type):
    train = list()
    test = list()
    case_times = list()

    log_end_time_filter = pm4py.filter_event_attribute_values(log, "timeinterval", ["t=3"], level="event", retain=True)

    #Append last event of case with its timestamp
    for trace in log_end_time_filter:
        case_times.append(case_time(trace.attributes['cdb_ec_id'], trace[-1]['time:timestamp']))

    #Sort the case ids by timestamp
    case_times = sorted(case_times, reverse=True)

    #Calculate train and test size
    train_len = int(len(case_times)*0.75)
    test_len = len(case_times)-train_len

    #Seperate train and test event log
    for idx, ct in enumerate(case_times):
        for trace in log:
            if(ct.case_id == trace.attributes['cdb_ec_id'] and idx <= train_len):
                train.append(trace)
            elif(ct.case_id == trace.attributes['cdb_ec_id'] and idx > train_len):
                test.append(trace)

    train_event_log = EventLog(train)
    test_event_log = EventLog(test)

    print(len(train_event_log))
    print(len(test_event_log))

    print(log_end_time_filter[train_len][-1]["time:timestamp"])
    print(log_end_time_filter[train_len+1][-1]["time:timestamp"])

    train_event_log.classifiers.update(log.classifiers)
    test_event_log.classifiers.update(log.classifiers)


    xes_exporter.apply(train_event_log, save_path+'/'+ process_type +'_Train.xes') #export for train event log
    xes_exporter.apply(test_event_log, save_path+'/'+ process_type +'_Test.xes') #export for test event log