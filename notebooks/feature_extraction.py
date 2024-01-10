from pm4py.objects.log.importer.xes import importer as xes_importer
import pandas as pd
import numpy as np

#load package for attribute filtering
from pm4py.algo.filtering.log.attributes import attributes_filter
import sys
import os

#Set util path
current = os.path.dirname(os.path.abspath(''))
print(current)
parent = os.path.dirname(current)
sys.path.append(parent)

#define gate types
gate6_types = ['Review & Release Gate 6']
prepare_gate6_types = ['Prepare the gate 6 release:']
gate7_types = ['Review & Release Gate 7']
prepare_gate7_types = ['Prepare Gate 7 Release']

gate6_7_types = ['Review & Release Gate 6&7', 'Review & Release Gate 7', 'Release gate 6+7', 'Review & Release Gate6&7']
prepare_gate6_7_types = ['Prepare Gate 6&7 Release', 'Prepare Gate 7 Release']
gate8_types = ['Review & Release Gate 8', 'Review&Release Gate 8', 'Review & release Gate 8 and all Dokuments']
prepare_gate8_types = ['Prepare Gate 8 Release', 'Prepare Gate 8 Loop', 'Prepare Gate 8 Loop (Editorial)']
gate9_types = ['Review & Release Gate 9', 'Release Gate 9']
prepare_gate9_types = ['Prepare Gate 9']


#GATES NONFFF
NONFFF_gates_and_T = [
    ('t=1', 'gate6_7'), 
    ('t=2', 'in_gate8'),
    (['t=1', 't=2'], 'until_gate8'),
    ('t=3', 'in_gate9'),
    (['t=1', 't=2', 't=3'], 'until_gate9')
]

NONFFF_UNTIL_gates_and_T = [
    ('t=1', 'gate6_7'),
    (['t=1', 't=2'], 'until_gate8'),
    (['t=1', 't=2', 't=3'], 'until_gate9')
]
#GATES EC
EC_gates_and_T = [
    ('t=1', 'gate6_7'), 
    ('t=2', 'in_gate8'),
    (['t=1', 't=2'], 'until_gate8')
]

EC_UNTIL_gates_and_T = [
    ('t=1', 'gate6_7'),
    (['t=1', 't=2'], 'until_gate8'),
]

#GATES FFF
FFF_gates_and_T = [
    ('t=1', 'gate6'),
    ('t=2', 'in_gate7'),
    (['t=1', 't=2'], 'until_gate7'),
    ('t=3', 'in_gate8'),
    (['t=1', 't=2', 't=3'], 'until_gate8'),
    ('t=4', 'in_gate9'),
    (['t=1', 't=2', 't=3', 't=4'], 'until_gate9')
]

FFF_UNTIL_gates_and_T = [
    ('t=1', 'gate6'),
    (['t=1', 't=2'], 'gate7'),
    (['t=1', 't=2', 't=3'], 'gate8'),
    (['t=1', 't=2', 't=3', 't=4'], 'gate9')
]



def feature_processed_documents(log, process_type):
    from bpmi40.WM_util import processed_cat_count
    from bpmi40.WM_util import TBC_Changed
    from bpmi40.WM_util import processed_documents
    from bpmi40.WM_util import document_changes
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T


    #Verarbeitete Dokumente der Kategorie 1
    for T_g in gates_and_T:
        log = processed_cat_count(log, T_g[0], T_g[1], asset="document", category="1")

    #Verarbeitete Dokumente der Kategorie 2
    for T_g in gates_and_T:
        log = processed_cat_count(log, T_g[0], T_g[1], asset="document", category="2")

    #Zusammenhang zwischen TBC Dokumenten und tatsächlich geänderten Dokumenten
    for T_g in UNTIL_gates_and_T:
        log = TBC_Changed(log, T_g[0], T_g[1], asset="document")

    #defines how many documents have been processed in each Gate/ until the completion of each gate
    for T_g in gates_and_T:
        log=processed_documents(log, T_g[0], T_g[1])

    #Anzahl der Dokumentenänderungen gesamt pro Stage
    for T_g in gates_and_T:
        log=document_changes(log, T_g[0], T_g[1])

    return log

def feature_number_of_parts(log, process_type):
    from bpmi40.WM_util import processed_cat_count
    from bpmi40.WM_util import TBC_Changed
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    #Number of parts in each part category
    for T_g in gates_and_T:
        log = processed_cat_count(log, T_g[0], T_g[1], asset="part")

    for T_g in UNTIL_gates_and_T:
        log = TBC_Changed(log, T_g[0], T_g[1], asset="part")

    return log

def feature_number_of_initiated_workflows(log, process_type):
    #number of initiated workflows is calcluated until the completion of Gate 6 & 7 (t=1), Gate 8 (t=2) & Gate 9 (t=3)
    from bpmi40.WM_util import number_initiated_workflows
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in UNTIL_gates_and_T:
        log=number_initiated_workflows(log, T_g[0], T_g[1])

    return log

def feature_number_of_different_roles(log, process_type):
    #number of different roles involved is calcluated within / until the completion of Gate 6 & 7, Gate 8 and Gate 9
    from bpmi40.WM_util import involved_roles
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in gates_and_T:
        log=involved_roles(log, T_g[0], T_g[1])

    return log

def feature_qma_check_relevance(log):
    from bpmi40.WM_util import qma_check_done
    from bpmi40.WM_util import qma_check_relevant

    log=qma_check_done(log, 'G7 - 6: QMA Check 1', '1')
    log=qma_check_relevant(log, 'G7 - 6: QMA Check 1', '1')

    return log


def t0_to_be_changed(log):
    from bpmi40.WM_util import to_be_changed
    from bpmi40.WM_util import TBC_cat_count

    log=to_be_changed(log, 'Document to be Changed', 'documents')
    log=to_be_changed(log, 'Part to be Changed', 'parts')

    log = TBC_cat_count(log, asset="document", category="1")
    log = TBC_cat_count(log, asset="document", category="2")
    log = TBC_cat_count(log, asset="part")
    
    return log

def t0_delete_parts_documents(log):
    #delete documents and parts from event log
    delete_task = ['Attached Document', 'Changed Document', 'Document to be Changed', 'Part to be Changed', 'Changed Part']
    for trace in log:
        for event in trace:
            if event["event_type"] in delete_task:
                event["event_type"] = 'Delete Task'

    #filter events by event type containing "Delete Task" and keep all events except those
    log = attributes_filter.apply_events(log, ['Delete Task'],parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: "event_type", attributes_filter.Parameters.POSITIVE: False})

    return log

def t0_start_division(log):
    #StartDivision
    from bpmi40.WM_util import start_division
    log = start_division(log)

    return log

def number_tailored_criteria(log, process_type):
    #NumberTailoredCriteria
    from bpmi40.WM_util import tailored_criteria
    eventtype_tailoredcriteria=['Task Event', 'Workflow Event']
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in gates_and_T:
        log=tailored_criteria(log, eventtype_tailoredcriteria, T_g[0], T_g[1])

    return log

def number_completed_tasks(log, process_type):
    #NumberTailoredCriteria
    from bpmi40.WM_util import completed_tasks
    eventtype_completedtasks=['Task Event', 'Workflow Event']
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in gates_and_T:
        log=completed_tasks(log, eventtype_completedtasks, T_g[0], T_g[1])

    return log

def number_added_tasks(log, process_type):
    #NumberAddedTasks
    from bpmi40.WM_util import added_tasks
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in gates_and_T:
        log=added_tasks(log, T_g[0], T_g[1])

    return log

def international_involved(log, process_type):
    #InternationalInvolved
    from bpmi40.WM_util import international_involved

    eventtype_internationalinvolved=['Task Event', 'Workflow Event']

    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in gates_and_T:
        log=international_involved(log, eventtype_internationalinvolved, T_g[0], T_g[1])

    return log


def involved_divisions_encoded(log, process_type):
    from bpmi40.WM_util import division

    eventtype_division=['Task Event', 'Workflow Event']

    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in gates_and_T:
        log=division(log, 'AC', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'AH', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'AI', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'AP', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'DI', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'IT', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'MR', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'R', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'R&', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'TA', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'TC', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'TD', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'TL', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'TM', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'TQ', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'TX', eventtype_division, T_g[0], T_g[1])
        log=division(log, 'WM', eventtype_division, T_g[0], T_g[1])

    return log

def number_divisions(log, process_type):
    from bpmi40.WM_util import number_divisions_involved
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in gates_and_T:
        log=number_divisions_involved(log, T_g[0], T_g[1])

    return log

def t0_start_month(log):
    from bpmi40.WM_util import start_month

    log = start_month(log)

    return log

def feature_last_three_events_per_case(log, process_type):
    from bpmi40.WM_util import event_list
    log = event_list(log, process_type)

    return log

def feature_involved_resources(log, process_type):
    from bpmi40.WM_util import resource_list
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in UNTIL_gates_and_T:
        log=resource_list(log, T_g[0], T_g[1], 50)

    return log

def feature_multiple_roles(log, process_type):
    from bpmi40.WM_util import resource_multiple_task
    global NONFFF_gates_and_T
    global NONFFF_UNTIL_gates_and_T
    global FFF_gates_and_T
    global FFF_UNTIL_gates_and_T
    global EC_gates_and_T
    global EC_UNTIL_gates_and_T
    gates_and_T = None
    UNTIL_gates_and_T = None
    if(process_type == 'NONFFF'):
        gates_and_T = NONFFF_gates_and_T
        UNTIL_gates_and_T = NONFFF_UNTIL_gates_and_T
    elif (process_type == 'EC'):
        gates_and_T = EC_gates_and_T
        UNTIL_gates_and_T = EC_UNTIL_gates_and_T        
    else:
        gates_and_T = FFF_gates_and_T
        UNTIL_gates_and_T = FFF_UNTIL_gates_and_T

    for T_g in gates_and_T:
        log=resource_multiple_task(log, T_g[0], T_g[1])

    return log

#Process Types EC, NONFFF, FFF
def calculate_lead_time_per_gate(log, process_type):
    from bpmi40.WM_util import leadtime
    global gate6_types
    global prepare_gate6_types
    global gate7_types
    global prepare_gate7_types

    global gate6_7_types
    global prepare_gate6_7_types
    global gate8_types
    global prepare_gate8_types
    global gate9_types
    global prepare_gate9_types

    if process_type == "NONFFF":
        #The lead time for gate 6 & 7 (beginning of the process until Gate 6 & 7 is completed) is calculated.
        log=leadtime(log, "first", 'start', gate6_7_types, 'complete', 'G6_7_leadtime')

        #The lead time for gate 8 (end of gate 7 until Gate 8 is completed) is calculated.
        log=leadtime(log, gate6_7_types, 'complete', gate8_types, 'complete', 'G8_leadtime')
        
        #The lead time for gate 9 (end of gate 8 until Gate 9 is completed) is calculated.
        log=leadtime(log, gate8_types, 'complete', gate9_types, 'complete', 'G9_leadtime')
    
    elif process_type == "EC":
        log=leadtime(log, "first", 'start', gate6_7_types, 'complete', 'G6_7_leadtime')
        log=leadtime(log, gate6_7_types, 'complete', gate8_types, 'complete', 'G8_leadtime')

    else:
        log=leadtime(log, "first", 'start', gate6_types, 'complete', 'G6_leadtime')
        log=leadtime(log, gate6_types, 'complete', gate7_types, 'complete', 'G7_leadtime')
        log=leadtime(log, gate7_types, 'complete', gate8_types, 'complete', 'G8_leadtime')
        log=leadtime(log, gate8_types, 'complete', gate9_types, 'complete', 'G9_leadtime')

    return log

###Process Types FFF, NONFFF, EC
def calculate_total_and_remaining_leadtime(log, process_type):
    ###Calculate total lead time
    from bpmi40.WM_util import total_leadtime
    global gate6_types
    global prepare_gate6_types
    global gate7_types
    global prepare_gate7_types

    global gate6_7_types
    global prepare_gate6_7_types
    global gate8_types
    global prepare_gate8_types
    global gate9_types
    global prepare_gate9_types

    log=total_leadtime(log)
    print(log[0].attributes["total_leadtime"])

    #Calculate remaining lead time from Gate 6 & 7, Gate 8 and Gate 9
    from bpmi40.WM_util import remaining_leadtime
    if process_type == 'FFF':
        log=remaining_leadtime(log, gate6_types, 'G6_remaining_leadtime')
        log=remaining_leadtime(log, gate7_types, 'G7_remaining_leadtime')
        log=remaining_leadtime(log, gate8_types, 'G8_remaining_leadtime')
        log=remaining_leadtime(log, gate9_types, 'G9_remaining_leadtime')
    elif process_type == 'EC':
        log=remaining_leadtime(log, gate6_7_types, 'G6_7_remaining_leadtime')
        log=remaining_leadtime(log, gate8_types, 'G8_remaining_leadtime')       
    else:
        log=remaining_leadtime(log, gate6_7_types, 'G6_7_remaining_leadtime')
        log=remaining_leadtime(log, gate8_types, 'G8_remaining_leadtime')
        log=remaining_leadtime(log, gate9_types, 'G9_remaining_leadtime')

    return log


def extract_and_save(log, savepath, log_type, process_type):
    if process_type == 'NONFFF':
        _extract_and_save_NONFFF(log, savepath, log_type)
    elif process_type == 'EC':
        _extract_and_save_EC(log, savepath, log_type)
    else:
        _extract_and_save_FFF(log, savepath, log_type)

def _extract_and_save_FFF(log, savepath, log_type, process_type='FFF'):
    from pm4py.algo.transformation.log_to_features import algorithm as log_to_features

    t0_special_names = set(["concept:name", "cdb_ec_id", "number_documents_to_be_changed", "number_parts_to_be_changed", "start_month",
    "c_event", "c_source", "start_division", "total_leadtime"])

    t1_special_names = set(["QMA_check1_not_relevant", "QMA_check1_done"])

    trace_level_attribute_names = set(log[0].attributes.keys()) - t1_special_names - t0_special_names
    print(trace_level_attribute_names)

    #T0
    t0 = list()
    t0.extend(t0_special_names)

    for feature in trace_level_attribute_names:
        if feature[:3] == "TBC":
            t0.append(feature)
    t0.append("G6_leadtime")
    t0 = list(set(t0))
    print(t0)

    #T1
    t1 = list()
    t1.extend(t0)
    t1.extend(t1_special_names)

    for feature in trace_level_attribute_names:
        if ("gate6" in feature) or ("G6" in feature) or ("T1" in feature):
            t1.append(feature)
    t1.append("G7_leadtime")
    t1 = list(set(t1))
    print(t1)

    #T2
    t2 = list()
    t2.extend(t1)

    for feature in trace_level_attribute_names:
        if ("gate7" in feature) or ("G7" in feature) or ("T2" in feature):
            t2.append(feature)
    t2.append("G8_leadtime")
    t2 = list(set(t2))
    print(t2)

    #T3
    t3 = list()
    t3.extend(t2)

    for feature in trace_level_attribute_names:
        if ("gate8" in feature) or ("G8" in feature) or ("T3" in feature):
            t3.append(feature)
    t3.append("G9_leadtime")
    t3 = list(set(t3))
    print(t3)

    #T4
    t4 = list()
    t4.extend(t3)

    for feature in trace_level_attribute_names:
        if ("gate9" in feature) or ("G9" in feature) or ("T4" in feature):
            t4.append(feature)
    t4 = list(set(t4))
    print(t4)

    print((set(t4) - t1_special_names - t0_special_names) - (trace_level_attribute_names))


    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t0})
    print(len(feature_names))

    #Feature Extraction for t=0
    FFF_features_t0 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    FFF_features_t0['trace:G6_leadtime'] = np.where(FFF_features_t0['trace:G6_leadtime'] <= 0, nan, FFF_features_t0['trace:G6_leadtime'])
    FFF_features_t0['trace:total_leadtime'] = np.where(FFF_features_t0['trace:total_leadtime'] <= 0, nan, FFF_features_t0['trace:total_leadtime'])

    FFF_features_t0.head(10)
    FFF_features_t0.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t0.csv', index=False)


    #Feature Extraction for t=1
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t1})
    print(len(feature_names))

    FFF_features_t1 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    FFF_features_t1['trace:G6_leadtime'] = np.where(FFF_features_t1['trace:G6_leadtime'] <= 0, nan, FFF_features_t1['trace:G6_leadtime'])
    FFF_features_t1['trace:G7_leadtime'] = np.where(FFF_features_t1['trace:G7_leadtime'] <= 0, nan, FFF_features_t1['trace:G7_leadtime'])
    FFF_features_t1['trace:G6_remaining_leadtime'] = np.where(FFF_features_t1['trace:G6_remaining_leadtime'] <= 0, nan, FFF_features_t1['trace:G6_remaining_leadtime'])
    FFF_features_t1['trace:total_leadtime'] = np.where(FFF_features_t1['trace:total_leadtime'] <= 0, nan, FFF_features_t1['trace:total_leadtime'])

    FFF_features_t1.head(10)

    FFF_features_t1.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t1.csv', index=False)

    # Feature Extraction for t=2
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t2})
    print(len(feature_names))

    FFF_features_t2 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    FFF_features_t2['trace:G6_leadtime'] = np.where(FFF_features_t2['trace:G6_leadtime'] <= 0, nan, FFF_features_t2['trace:G6_leadtime'])
    FFF_features_t2['trace:G7_leadtime'] = np.where(FFF_features_t2['trace:G7_leadtime'] <= 0, nan, FFF_features_t2['trace:G7_leadtime'])
    FFF_features_t2['trace:G8_leadtime'] = np.where(FFF_features_t2['trace:G8_leadtime'] <= 0, nan, FFF_features_t2['trace:G8_leadtime'])
    FFF_features_t2['trace:G7_remaining_leadtime'] = np.where(FFF_features_t2['trace:G7_remaining_leadtime'] <= 0, nan, FFF_features_t2['trace:G7_remaining_leadtime'])
    FFF_features_t2['trace:total_leadtime'] = np.where(FFF_features_t2['trace:total_leadtime'] <= 0, nan, FFF_features_t2['trace:total_leadtime'])

    FFF_features_t2.head(10)

    FFF_features_t2.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t2.csv', index=False)

    # Feature Extraction for t=3
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t3})
    print(len(feature_names))

    FFF_features_t3 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    FFF_features_t3['trace:G6_leadtime'] = np.where(FFF_features_t3['trace:G6_leadtime'] <= 0, nan, FFF_features_t3['trace:G6_leadtime'])
    FFF_features_t3['trace:G7_leadtime'] = np.where(FFF_features_t3['trace:G7_leadtime'] <= 0, nan, FFF_features_t3['trace:G7_leadtime'])
    FFF_features_t3['trace:G8_leadtime'] = np.where(FFF_features_t3['trace:G8_leadtime'] <= 0, nan, FFF_features_t3['trace:G8_leadtime'])
    FFF_features_t3['trace:G9_leadtime'] = np.where(FFF_features_t3['trace:G9_leadtime'] <= 0, nan, FFF_features_t3['trace:G9_leadtime'])
    FFF_features_t3['trace:G8_remaining_leadtime'] = np.where(FFF_features_t3['trace:G8_remaining_leadtime'] <= 0, nan, FFF_features_t3['trace:G8_remaining_leadtime'])
    FFF_features_t3['trace:total_leadtime'] = np.where(FFF_features_t3['trace:total_leadtime'] <= 0, nan, FFF_features_t3['trace:total_leadtime'])

    FFF_features_t3.head(10)

    FFF_features_t3.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t3.csv', index=False)

    # Feature Extraction for t=4
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t4})
    print(len(feature_names))

    FFF_features_t4 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    FFF_features_t4['trace:G6_leadtime'] = np.where(FFF_features_t4['trace:G6_leadtime'] <= 0, nan, FFF_features_t4['trace:G6_leadtime'])
    FFF_features_t4['trace:G7_leadtime'] = np.where(FFF_features_t4['trace:G7_leadtime'] <= 0, nan, FFF_features_t4['trace:G7_leadtime'])
    FFF_features_t4['trace:G8_leadtime'] = np.where(FFF_features_t4['trace:G8_leadtime'] <= 0, nan, FFF_features_t4['trace:G8_leadtime'])
    FFF_features_t4['trace:G9_leadtime'] = np.where(FFF_features_t4['trace:G9_leadtime'] <= 0, nan, FFF_features_t4['trace:G9_leadtime'])
    FFF_features_t4['trace:G9_remaining_leadtime'] = np.where(FFF_features_t4['trace:G9_remaining_leadtime'] <= 0, nan, FFF_features_t4['trace:G9_remaining_leadtime'])
    FFF_features_t4['trace:total_leadtime'] = np.where(FFF_features_t4['trace:total_leadtime'] <= 0, nan, FFF_features_t4['trace:total_leadtime'])

    FFF_features_t4.head(10)
    FFF_features_t4.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t4.csv', index=False)

def _extract_and_save_NONFFF(log, savepath, log_type, process_type='NONFFF'):
    from pm4py.algo.transformation.log_to_features import algorithm as log_to_features

    t0_special_names = set(["concept:name", "cdb_ec_id", "number_documents_to_be_changed", "number_parts_to_be_changed", "start_month",
    "c_event", "c_source", "start_division", "total_leadtime"])

    t1_special_names = set(["QMA_check1_not_relevant", "QMA_check1_done"])

    trace_level_attribute_names = set(log[0].attributes.keys()) - t1_special_names - t0_special_names
    print(trace_level_attribute_names)

    #T0
    t0 = list()
    t0.extend(t0_special_names)

    for feature in trace_level_attribute_names:
        if feature[:3] == "TBC":
            t0.append(feature)
    t0.append("G6_7_leadtime")
    t0 = list(set(t0))
    print(t0)

    #T1
    t1 = list()
    t1.extend(t0)
    t1.extend(t1_special_names)

    for feature in trace_level_attribute_names:
        if ("gate6_7" in feature) or ("G6_7_" in feature) or ("T1" in feature):
            t1.append(feature)
    t1.append("G8_leadtime")
    t1 = list(set(t1))
    print(t1)

    #T2
    t2 = list()
    t2.extend(t1)

    for feature in trace_level_attribute_names:
        if ("gate8" in feature) or ("G8" in feature) or ("T2" in feature):
            t2.append(feature)
    t2.append("G9_leadtime")
    t2 = list(set(t2))
    print(t2)

    #T3
    t3 = list()
    t3.extend(t2)

    for feature in trace_level_attribute_names:
        if ("gate9" in feature) or ("G9" in feature) or ("T3" in feature):
            t3.append(feature)
    t3 = list(set(t3))
    print(t3)

    print((set(t3) - t1_special_names - t0_special_names) - (trace_level_attribute_names))


    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t0})
    print(len(feature_names))

    #Feature Extraction for t=0
    NONFFF_features_t0 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    NONFFF_features_t0['trace:G6_7_leadtime'] = np.where(NONFFF_features_t0['trace:G6_7_leadtime'] <= 0, nan, NONFFF_features_t0['trace:G6_7_leadtime'])
    NONFFF_features_t0['trace:total_leadtime'] = np.where(NONFFF_features_t0['trace:total_leadtime'] <= 0, nan, NONFFF_features_t0['trace:total_leadtime'])

    NONFFF_features_t0.head(10)
    NONFFF_features_t0.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t0.csv', index=False)


    # Feature Extraction for t=1
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t1})
    print(len(feature_names))

    NONFFF_features_t1 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    NONFFF_features_t1['trace:G6_7_leadtime'] = np.where(NONFFF_features_t1['trace:G6_7_leadtime'] <= 0, nan, NONFFF_features_t1['trace:G6_7_leadtime'])
    NONFFF_features_t1['trace:G8_leadtime'] = np.where(NONFFF_features_t1['trace:G8_leadtime'] <= 0, nan, NONFFF_features_t1['trace:G8_leadtime'])
    NONFFF_features_t1['trace:G6_7_remaining_leadtime'] = np.where(NONFFF_features_t1['trace:G6_7_remaining_leadtime'] <= 0, nan, NONFFF_features_t1['trace:G6_7_remaining_leadtime'])
    NONFFF_features_t1['trace:total_leadtime'] = np.where(NONFFF_features_t1['trace:total_leadtime'] <= 0, nan, NONFFF_features_t1['trace:total_leadtime'])

    NONFFF_features_t1.head(10)
    NONFFF_features_t1.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t1.csv', index=False)


    # Feature Extraction for t=2
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t2})
    print(len(feature_names))

    NONFFF_features_t2 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    NONFFF_features_t2['trace:G6_7_leadtime'] = np.where(NONFFF_features_t2['trace:G6_7_leadtime'] <= 0, nan, NONFFF_features_t2['trace:G6_7_leadtime'])
    NONFFF_features_t2['trace:G8_leadtime'] = np.where(NONFFF_features_t2['trace:G8_leadtime'] <= 0, nan, NONFFF_features_t2['trace:G8_leadtime'])
    NONFFF_features_t2['trace:G9_leadtime'] = np.where(NONFFF_features_t2['trace:G9_leadtime'] <= 0, nan, NONFFF_features_t2['trace:G9_leadtime'])
    NONFFF_features_t2['trace:G8_remaining_leadtime'] = np.where(NONFFF_features_t2['trace:G8_remaining_leadtime'] <= 0, nan, NONFFF_features_t2['trace:G8_remaining_leadtime'])
    NONFFF_features_t2['trace:total_leadtime'] = np.where(NONFFF_features_t2['trace:total_leadtime'] <= 0, nan, NONFFF_features_t2['trace:total_leadtime'])
    
    NONFFF_features_t2.head(10)

    NONFFF_features_t2.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t2.csv', index=False)

    # # Feature Extraction for t=3
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t3})
    print(len(feature_names))

    NONFFF_features_t3 = pd.DataFrame(data, columns=feature_names)


    #Replace negative leadtimes
    nan = float('NaN')
    NONFFF_features_t3['trace:G6_7_leadtime'] = np.where(NONFFF_features_t3['trace:G6_7_leadtime'] <= 0, nan, NONFFF_features_t3['trace:G6_7_leadtime'])
    NONFFF_features_t3['trace:G8_leadtime'] = np.where(NONFFF_features_t3['trace:G8_leadtime'] <= 0, nan, NONFFF_features_t3['trace:G8_leadtime'])
    NONFFF_features_t3['trace:G9_leadtime'] = np.where(NONFFF_features_t3['trace:G9_leadtime'] <= 0, nan, NONFFF_features_t3['trace:G9_leadtime'])
    NONFFF_features_t3['trace:G9_remaining_leadtime'] = np.where(NONFFF_features_t3['trace:G9_remaining_leadtime'] <= 0, nan, NONFFF_features_t3['trace:G9_remaining_leadtime'])
    NONFFF_features_t3['trace:total_leadtime'] = np.where(NONFFF_features_t3['trace:total_leadtime'] <= 0, nan, NONFFF_features_t3['trace:total_leadtime'])


    NONFFF_features_t3.head(10)

    NONFFF_features_t3.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t3.csv', index=False)

def _extract_and_save_EC(log, savepath, log_type, process_type='EC'):
    from pm4py.algo.transformation.log_to_features import algorithm as log_to_features

    t0_special_names = set(["concept:name", "cdb_ec_id", "number_documents_to_be_changed", "number_parts_to_be_changed", "start_month",
    "c_event", "c_source", "start_division", "total_leadtime"])

    t1_special_names = set(["QMA_check1_not_relevant", "QMA_check1_done"])

    trace_level_attribute_names = set(log[0].attributes.keys()) - t1_special_names - t0_special_names
    print(trace_level_attribute_names)

    #T0
    t0 = list()
    t0.extend(t0_special_names)

    for feature in trace_level_attribute_names:
        if feature[:3] == "TBC":
            t0.append(feature)
    t0.append("G6_7_leadtime")
    t0 = list(set(t0))
    print(t0)

    #T1
    t1 = list()
    t1.extend(t0)
    t1.extend(t1_special_names)

    for feature in trace_level_attribute_names:
        if ("gate6_7" in feature) or ("G6_7_" in feature) or ("T1" in feature):
            t1.append(feature)
    t1.append("G8_leadtime")
    t1 = list(set(t1))
    print(t1)

    #T2
    t2 = list()
    t2.extend(t1)

    for feature in trace_level_attribute_names:
        if ("gate8" in feature) or ("G8" in feature) or ("T2" in feature):
            t2.append(feature)
    t2 = list(set(t2))
    print(t2)

    print((set(t2) - t1_special_names - t0_special_names) - (trace_level_attribute_names))


    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t0})
    print(len(feature_names))

    #Feature Extraction for t=0
    NONFFF_features_t0 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    NONFFF_features_t0['trace:G6_7_leadtime'] = np.where(NONFFF_features_t0['trace:G6_7_leadtime'] <= 0, nan, NONFFF_features_t0['trace:G6_7_leadtime'])
    NONFFF_features_t0['trace:total_leadtime'] = np.where(NONFFF_features_t0['trace:total_leadtime'] <= 0, nan, NONFFF_features_t0['trace:total_leadtime'])

    NONFFF_features_t0.head(10)
    NONFFF_features_t0.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t0.csv', index=False)


    # Feature Extraction for t=1
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t1})
    print(len(feature_names))

    NONFFF_features_t1 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    NONFFF_features_t1['trace:G6_7_leadtime'] = np.where(NONFFF_features_t1['trace:G6_7_leadtime'] <= 0, nan, NONFFF_features_t1['trace:G6_7_leadtime'])
    NONFFF_features_t1['trace:G8_leadtime'] = np.where(NONFFF_features_t1['trace:G8_leadtime'] <= 0, nan, NONFFF_features_t1['trace:G8_leadtime'])
    NONFFF_features_t1['trace:G6_7_remaining_leadtime'] = np.where(NONFFF_features_t1['trace:G6_7_remaining_leadtime'] <= 0, nan, NONFFF_features_t1['trace:G6_7_remaining_leadtime'])
    NONFFF_features_t1['trace:total_leadtime'] = np.where(NONFFF_features_t1['trace:total_leadtime'] <= 0, nan, NONFFF_features_t1['trace:total_leadtime'])

    NONFFF_features_t1.head(10)
    NONFFF_features_t1.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t1.csv', index=False)


    # Feature Extraction for t=2
    data, feature_names = log_to_features.apply(log, parameters={"num_tr_attr": t2})
    print(len(feature_names))

    NONFFF_features_t2 = pd.DataFrame(data, columns=feature_names)

    #Replace negative leadtimes
    nan = float('NaN')
    NONFFF_features_t2['trace:G6_7_leadtime'] = np.where(NONFFF_features_t2['trace:G6_7_leadtime'] <= 0, nan, NONFFF_features_t2['trace:G6_7_leadtime'])
    NONFFF_features_t2['trace:G8_leadtime'] = np.where(NONFFF_features_t2['trace:G8_leadtime'] <= 0, nan, NONFFF_features_t2['trace:G8_leadtime'])
    NONFFF_features_t2['trace:G8_remaining_leadtime'] = np.where(NONFFF_features_t2['trace:G8_remaining_leadtime'] <= 0, nan, NONFFF_features_t2['trace:G8_remaining_leadtime'])
    NONFFF_features_t2['trace:total_leadtime'] = np.where(NONFFF_features_t2['trace:total_leadtime'] <= 0, nan, NONFFF_features_t2['trace:total_leadtime'])
    
    NONFFF_features_t2.head(10)

    NONFFF_features_t2.to_csv(savepath+'/'+ process_type +'_'+log_type+'_Features_t2.csv', index=False)

