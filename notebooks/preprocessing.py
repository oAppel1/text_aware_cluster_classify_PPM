import pm4py
import pandas as pd
import numpy as np

#Import for sorting by timestamp
from pm4py.objects.log.util import sorting

#Specify all necessary keyword to differentiate between documents and parts
document_types = ['Document to be Changed', 'Changed Document', 'Attached Document']
part_types = ['Part to be Changed', 'Changed Part']

#Gate Names EC
gate6_7_types_EC = ['Review & Release Gate 6&7', 'Prepare Gate 6&7 Release', 'Review & Release Gate6&7', 'Release gate 6+7',
                        'Prepare the gate 6 release:', 'Review & Release Gate 6', 'Prepare Gate 7 Release', 'Review & Release Gate 7']
    
gate8_types_EC = ['Review & Release Gate 8', 'Prepare Gate 8 Loop', 
                    'Prepare Gate 8 Release', 'Review&Release Gate 8', 
                    'Prepare Gate 8 Loop (Editorial)',
                    'Review & Release Gate 8 and all Dokuments']

gate9_types_EC = ['Prepare Gate 9', 'Release Gate 9', 'Review & Release Gate 9']

#Gate Names FFF
gate6_types_FFF = ['Review & Release Gate 6', 'Prepare the gate 6 release:']
gate7_types_FFF = ['Review & Release Gate 7', 'Prepare Gate 7 Release']
gate8_types_FFF = ['Review & Release Gate 8', 'Prepare Gate 8 Release', 'Prepare Gate 8 Loop']
gate9_types_FFF = ['Review & Release Gate 9', 'Prepare Gate 9', 'Release Gate 9']

#Gate Names NONFFF
gate6_7_types_NONFFF = ['Review & Release Gate 6&7', 'Prepare Gate 6&7 Release', 'Review & Release Gate 6', 'Review & Release Gate 7', 'Prepare the gate 6 release:',
                        'Prepare Gate 7 Release', 'Review & Release Gate 6/7']
    
gate8_types_NONFFF = ['Review & Release Gate 8', 'Prepare Gate 8 Release', 'Prepare Gate 8 Loop']

gate9_types_NONFFF = ['Review & Release Gate 9', 'Prepare Gate 9', 'Release Gate 9']

#same for every type of log
def modify_timestamps(log):
    global document_types
    global part_types

    print(document_types)
    print(part_types)

    doc_time_key = "document:modified"
    part_time_key = "part:modified"

    #Modify timestamps for NONFFF
    for trace in log:
        for event in trace:
            if(event["event_type"] in document_types):
                event["time:timestamp"] = event[doc_time_key]
            elif(event["event_type"] in part_types):
                event["time:timestamp"] = event[part_time_key]

    return log

#same for every log
def drop_events(log, ec=False):
    global document_types
    global part_types

    #Define which Events should be dropped from the event log
    for trace in log:
        for event in trace:
            if(event["event_type"]=='Task Event'):
                if(event["description"][:6]=='Copied' or
                   event["description"][:5]=='Added' or
                   event["description"][:6]=='Delete' or
                   event["description"][:11]=='Hinzugefügt' or
                   event["description"][:7]=='Kopiert' or
                   event["description"][:8]=='Gelöscht' or 
                   event["cdb_process_id"]==event["concept:name"]):
                    
                    event["description"] = 'Delete Task'

    #ECs drop empty events
    for trace in log:
        time_ex = pm4py.filter_trace(lambda x: "time:timestamp" not in x, trace)
        for event in time_ex:
            event["description"] = 'Delete Task'

    #Specify all necessary keywords to differentiate between documents and parts and add a description key to those. 
    #Only then the next function can iterate through all events to identify which tasks have to be deleted
    #add a description key to documents and parts
    for trace in log:
        for event in trace:
            if(event["event_type"] in document_types):
                event["description"] = event["concept:name"]
            if(event["event_type"] in part_types):
                event["description"] = event["concept:name"]

    for trace in log:
        for event in trace:
            try:
                description = event["description"]
            except:
                print(trace.attributes["cdb_ec_id"])
                event["description"] = event["concept:name"]
                print(event["concept:name"])

    #filter events by description containing "Delete Task" and keep all events except those
    log = pm4py.filter_event_attribute_values(log, "description", ['Delete Task'], level="event", retain=False)

    #delete the description key from documents and parts (not necessary because its identical to their concept:name like defined before)
    for trace in log:
        for event in trace:
            if(event["event_type"] in document_types):
                del event["description"]
            if(event["event_type"] in part_types):
                del event["description"]
    
    return log

#same for every type of log
def adjust_concept_names(log):
    global document_types
    global part_types

    #change concept:name of documents, parts and workflow events
    for trace in log:
        for event in trace: 
            if event["event_type"] in document_types:
                event["concept:name"]=event["event_type"]+' - '+event["document:categ1"]
            if event["event_type"] in part_types:
                event["concept:name"]=event["event_type"]+' - '+event["part:categ"]
            if event["event_type"] == "Workflow Event":
                event["concept:name"]=event["event_type"]

    return log

#same for every type of log
def refine_activity_classifier(log):
    #refine activity classifier
    log.classifiers['Activity classifier']
    log.classifiers.update({"Activity classifier":["concept:name"]})
    for trace in log:
        for event in trace:
            event["@@classifier"] = event["concept:name"]

    return log

def assign_gates_EC_With_Event_Log_Loop(log_EC):
    global gate6_7_types_EC
    global gate8_types_EC
    global gate9_types_EC

    for trace in log_EC:
        gate6_7_check = False
        gate8_check = False
        gate9_check = False
        for event in trace:
            if event["concept:name"] in gate6_7_types_EC and event["lifecycle:transition"] == event["lifecycle:transition"]=="complete":
                gate6_7_check = True
            
            if event["concept:name"] in gate8_types_EC and event["lifecycle:transition"] == event["lifecycle:transition"]=="complete":
                gate8_check = True

            if event["concept:name"] in gate9_types_EC:
                gate9_check = True
        
        if gate9_check:
            trace.attributes["delete"] = "True"
        elif gate6_7_check and gate8_check:
            trace.attributes["delete"] = "False"
        else:
            trace.attributes["delete"] = "True"

    log_EC = pm4py.filter_trace_attribute_values(log_EC, "delete", ["True"], retain=False)

    #Sort EC
    log_EC = sorting.sort_timestamp(log_EC, reverse_sort=False)

    for trace in log_EC:
        i = 0
        idx_g6_7 = 0
        idx_g8 = 0

        for event in trace:
            if (event["concept:name"] in gate6_7_types_EC) and (event["lifecycle:transition"]=="complete"):
                idx_g6_7 = i
            elif (event["concept:name"] in gate8_types_EC) and (event["lifecycle:transition"]=="complete"):
                idx_g8 = i
            
            i+=1

        if  idx_g8 < idx_g6_7:
            print(trace.attributes['concept:name'])
            print(trace[idx_g6_7])
            print(trace[idx_g8])

        for event in trace:
            if ((event["time:timestamp"].timestamp() <= trace[idx_g6_7]["time:timestamp"].timestamp()) and 
            (event["time:timestamp"].timestamp() >= trace[0]["time:timestamp"].timestamp())):
                event["timeinterval"] = 't=1'

            elif ((event["time:timestamp"].timestamp() <= trace[idx_g8]["time:timestamp"].timestamp()) and 
            (event["time:timestamp"].timestamp() > trace[idx_g6_7]["time:timestamp"].timestamp())):
                event["timeinterval"] = 't=2'

            elif (event["time:timestamp"].timestamp() > trace[idx_g8]["time:timestamp"].timestamp()):
                    event["timeinterval"] = 't=3'

    return log_EC

def assign_gates_EC(log_EC):
    global gate6_7_types_EC
    global gate8_types_EC
    global gate9_types_EC

    for trace in log_EC:
        lifecycle_missing = pm4py.filter_trace(lambda x: "lifecycle:transition" not in x, trace)
        for event in lifecycle_missing:
            event["lifecycle:transition"] = 'PLACEHOLDER'

    #Delete processes that went through Gate 9
    filter_gate9 = pm4py.filter_event_attribute_values(log_EC, 'concept:name', gate9_types_EC,level="case", retain=True)
    case_ids_gate9 = list(pm4py.get_trace_attribute_values(filter_gate9, "cdb_ec_id").keys())

    log_EC = pm4py.filter_trace_attribute_values(log_EC, 'cdb_ec_id', case_ids_gate9, retain=False)

    ###Only complete until Gate 8
    filter_complete = pm4py.filter_event_attribute_values(log_EC, 'lifecycle:transition', ["complete"], level='event', retain=True)

    filter_complete_gate6_7 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate6_7_types_EC, level='case', retain=True)
    filter_complete_gate8 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate8_types_EC, level='case', retain=True)

    case_ids_gate6_7 = list(pm4py.get_trace_attribute_values(filter_complete_gate6_7, "cdb_ec_id").keys())
    case_ids_gate8 = list(pm4py.get_trace_attribute_values(filter_complete_gate8, "cdb_ec_id").keys())

    case_ids_to_keep = list(set(case_ids_gate6_7) & set(case_ids_gate8))
    print(len(case_ids_to_keep))

    log_EC = pm4py.filter_trace_attribute_values(log_EC, 'cdb_ec_id', case_ids_to_keep, retain=True)

    #Sort EC
    log_EC = sorting.sort_timestamp(log_EC, reverse_sort=False)

    for trace in log_EC:
            i = 0
            idx_g6_7 = 0
            idx_g8 = 0

            for event in trace:
                if (event["concept:name"] in gate6_7_types_EC) and (event["lifecycle:transition"]=="complete"):
                    idx_g6_7 = i
                elif (event["concept:name"] in gate8_types_EC) and (event["lifecycle:transition"]=="complete"):
                    idx_g8 = i
                
                i+=1

            if  idx_g8 < idx_g6_7:
                print(trace.attributes['concept:name'])
                print(trace[idx_g6_7])
                print(trace[idx_g8])

            for event in trace:
                if ((event["time:timestamp"].timestamp() <= trace[idx_g6_7]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() >= trace[0]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=1'

                elif ((event["time:timestamp"].timestamp() <= trace[idx_g8]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() > trace[idx_g6_7]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=2'

                elif (event["time:timestamp"].timestamp() > trace[idx_g8]["time:timestamp"].timestamp()):
                    event["timeinterval"] = 't=3'

    return log_EC

def assign_gates_NONFFF(log_NONFFF):
    global gate6_7_types_NONFFF
    global gate8_types_NONFFF
    global gate9_types_NONFFF

    for trace in log_NONFFF:
        lifecycle_missing = pm4py.filter_trace(lambda x: "lifecycle:transition" not in x, trace)
        for event in lifecycle_missing:
            event["lifecycle:transition"] = 'PLACEHOLDER'

    ###Only complete until Gate 8
    filter_complete = pm4py.filter_event_attribute_values(log_NONFFF, 'lifecycle:transition', ["complete"], level='event', retain=True)

    filter_complete_gate6_7 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate6_7_types_NONFFF, level='case', retain=True)
    filter_complete_gate8 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate8_types_NONFFF, level='case', retain=True)
    filter_complete_gate9 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate9_types_NONFFF, level='case', retain=True)

    case_ids_gate6_7 = list(pm4py.get_trace_attribute_values(filter_complete_gate6_7, "cdb_ec_id").keys())
    case_ids_gate8 = list(pm4py.get_trace_attribute_values(filter_complete_gate8, "cdb_ec_id").keys())
    case_ids_gate9 = list(pm4py.get_trace_attribute_values(filter_complete_gate9, "cdb_ec_id").keys())

    case_ids_to_keep = list(set(case_ids_gate6_7) & set(case_ids_gate8) & set(case_ids_gate9))
    print(len(case_ids_to_keep))

    log_NONFFF = pm4py.filter_trace_attribute_values(log_NONFFF, 'cdb_ec_id', case_ids_to_keep, retain=True)

    #Sort NONFFF
    log_NONFFF = sorting.sort_timestamp(log_NONFFF, reverse_sort=False)

    for trace in log_NONFFF:
            i = 0
            idx_g6_7 = 0
            idx_g8 = 0
            idx_g9 = 0

            for event in trace:
                if (event["concept:name"] in gate6_7_types_NONFFF) and (event["lifecycle:transition"]=="complete"):
                    idx_g6_7 = i
                elif (event["concept:name"] in gate8_types_NONFFF) and (event["lifecycle:transition"]=="complete"):
                    idx_g8 = i
                elif (event["concept:name"] in gate9_types_NONFFF) and (event["lifecycle:transition"]=="complete"):
                    idx_g9 = i

                i+=1

            if  idx_g8 < idx_g6_7 or idx_g9 < idx_g8 or idx_g9 < idx_g6_7:
                print(trace.attributes['concept:name'])
                print(trace[idx_g6_7])
                print(trace[idx_g8])
                print(trace[idx_g9])

            for event in trace:
                if ((event["time:timestamp"].timestamp() <= trace[idx_g6_7]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() >= trace[0]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=1'

                elif ((event["time:timestamp"].timestamp() <= trace[idx_g8]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() > trace[idx_g6_7]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=2'

                elif ((event["time:timestamp"].timestamp() <= trace[idx_g9]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() > trace[idx_g8]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=3'

                elif (event["time:timestamp"].timestamp() > trace[idx_g9]["time:timestamp"].timestamp()):
                    event["timeinterval"] = 't=4'

    return log_NONFFF

def assign_gates_FFF(log_FFF):
    global gate6_types_FFF
    global gate7_types_FFF
    global gate8_types_FFF
    global gate9_types_FFF

    for trace in log_FFF:
        lifecycle_missing = pm4py.filter_trace(lambda x: "lifecycle:transition" not in x, trace)
        for event in lifecycle_missing:
            event["lifecycle:transition"] = 'PLACEHOLDER'

    ###Only complete until Gate 8
    filter_complete = pm4py.filter_event_attribute_values(log_FFF, 'lifecycle:transition', ["complete"], level='event', retain=True)

    filter_complete_gate6 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate6_types_FFF, level='case', retain=True)
    filter_complete_gate7 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate7_types_FFF, level='case', retain=True)
    filter_complete_gate8 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate8_types_FFF, level='case', retain=True)
    filter_complete_gate9 = pm4py.filter_event_attribute_values(filter_complete, 'concept:name', gate9_types_FFF, level='case', retain=True)

    case_ids_gate6 = list(pm4py.get_trace_attribute_values(filter_complete_gate6, "cdb_ec_id").keys())
    case_ids_gate7 = list(pm4py.get_trace_attribute_values(filter_complete_gate7, "cdb_ec_id").keys())
    case_ids_gate8 = list(pm4py.get_trace_attribute_values(filter_complete_gate8, "cdb_ec_id").keys())
    case_ids_gate9 = list(pm4py.get_trace_attribute_values(filter_complete_gate9, "cdb_ec_id").keys())

    case_ids_to_keep = list(set(case_ids_gate6) & set(case_ids_gate7) & set(case_ids_gate8) & set(case_ids_gate9))
    print(len(case_ids_to_keep))

    log_FFF = pm4py.filter_trace_attribute_values(log_FFF, 'cdb_ec_id', case_ids_to_keep, retain=True)

    #Sort FFF
    log_FFF = sorting.sort_timestamp(log_FFF, reverse_sort=False)

    for trace in log_FFF:
            i = 0
            idx_g6 = 0
            idx_g7 = 0
            idx_g8 = 0
            idx_g9 = 0

            for event in trace:
                if (event["concept:name"] in gate6_types_FFF) and (event["lifecycle:transition"]=="complete"):
                    idx_g6 = i
                elif (event["concept:name"] in gate7_types_FFF) and (event["lifecycle:transition"]=="complete"):
                    idx_g7 = i
                elif (event["concept:name"] in gate8_types_FFF) and (event["lifecycle:transition"]=="complete"):
                    idx_g8 = i
                elif (event["concept:name"] in gate9_types_FFF) and (event["lifecycle:transition"]=="complete"):
                    idx_g9 = i

                i+=1

            if  idx_g7 < idx_g6 or idx_g8 < idx_g7 or idx_g8 < idx_g6 or idx_g9 < idx_g8 or idx_g9 < idx_g7 or idx_g9 < idx_g6:
                print(trace.attributes['concept:name'])
                print(trace[idx_g6])
                print(trace[idx_g7])
                print(trace[idx_g8])
                print(trace[idx_g9])

            for event in trace:
                if ((event["time:timestamp"].timestamp() <= trace[idx_g6]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() >= trace[0]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=1'

                elif ((event["time:timestamp"].timestamp() <= trace[idx_g7]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() > trace[idx_g6]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=2'

                elif ((event["time:timestamp"].timestamp() <= trace[idx_g8]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() > trace[idx_g7]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=3'

                elif ((event["time:timestamp"].timestamp() <= trace[idx_g9]["time:timestamp"].timestamp()) and 
                (event["time:timestamp"].timestamp() > trace[idx_g8]["time:timestamp"].timestamp())):
                    event["timeinterval"] = 't=4'

                elif (event["time:timestamp"].timestamp() > trace[idx_g9]["time:timestamp"].timestamp()):
                    event["timeinterval"] = 't=5'

    return log_FFF

def remove_incomplete_traces(log, process_type):
    log = pm4py.filter_event_attribute_values(log, "timeinterval", ["t=1"], level="case", retain=True)
    log = pm4py.filter_event_attribute_values(log, "timeinterval", ["t=2"], level="case", retain=True)

    if process_type == "FFF" or process_type == "NONFFF":
        log = pm4py.filter_event_attribute_values(log, "timeinterval", ["t=3"], level="case", retain=True)

    if process_type == "FFF":
        log = pm4py.filter_event_attribute_values(log, "timeinterval", ["t=4"], level="case", retain=True)
        
    
    return log