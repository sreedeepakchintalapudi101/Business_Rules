import os
import json
import pandas as pd
import ast
import psutil
import requests

from flask import Flask, request, jsonify
from db_utils import DB
from ace_logger import Logging
from app import app
#
from .BusinessRules import BusinessRules
from time import time as tt
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string

logging = Logging(name='business_rules_api')

# Database configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}

def http_transport(encoded_span):
    # Validate input
    if not isinstance(encoded_span, (bytes, bytearray)):
        logging.error("Invalid input: 'encoded_span' must be bytes.")
        return
 
    try:
        # Use HTTPS for security
        response = requests.post(
            'https://servicebridge:80/zipkin',  # Change to HTTPS
            data=encoded_span,
            headers={'Content-Type': 'application/x-thrift'},
            timeout=5  # Set a timeout
        )
        # Handle response status
        if response.status_code != 200:
            logging.error(f"Failed to send data: {response.status_code} - {response.text}")
 
    except requests.RequestException as e:
        logging.error("An error occurred while sending the request.")
        logging.debug(f"Error details: {e}")  # Log details in debug level

def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes

def insert_into_audit(data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True


####################### FUNCTIONS ##################
################ Ideally should be in another file ### have to check why imports are not working

from difflib import SequenceMatcher

def partial_match(input_string, matchable_strings, threshold=75):
    """Returns the most similar string to the input_string from a list of strings.
    Args:
        input_string (str) -> the string which we have to compare.
        matchable_strings (list[str]) -> the list of strings from which we have to choose the most similar input_string.
        threshold (float) -> the threshold which the input_string should be similar to a string in matchable_strings.
    Example:
        sat = partial_match('lucif',['chandler','Lucifer','ross geller'])"""
    
    logging.info(f"input_string is {input_string}")
    logging.info(f"matchable_strings got are {matchable_strings}")
    result = {}
    words = matchable_strings
    match_word = input_string
    logging.info(f"words got for checking match are : {words}")
    max_ratio = 0
    match_got = ""
    for word in words:
        try:
            ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
            if ratio > 75 and ratio > max_ratio:
                max_ratio = ratio
                match_got = word
                logging.info(match_got)
        except Exception as e:
            logging.error("cannnot find match")
            logging.error(e)
            result['flag'] = 'False'
            result['data'] = {'reason':'got wrong input for partial match','error_msg':str(e)}
            return result
    if match_got:
        logging.info(f"match is {match_got} and ratio is {max_ratio}")
        result['flag'] = 'True'
        result['data'] = {'value':match_got}
    else:
        logging.info(f"match is {match_got} and ratio is {max_ratio}")
        result['flag'] = 'False'
        result['data'] = {'reason':f'no string is partial match morethan {threshold}%','error_msg':'got empty result'}
    return result

def date_transform(date, input_format='dd-mm-yyyy', output_format='dd-mm-yyyy'):
    """Date format change util function
    Args:
        date (str) -> the date string which needs to be converted
        input_format (str) -> the input format in which the given date string is present.
        output_format (str) -> the output format to which we have to convert the data into.
    Returns:
        result (dict)
        result is a dict having keys flag, data.Depending on the flag the data changes.
        
        if flag is True: data contains the key value and value is the converted date.
        if flag is False: data contains the error_msg say why its failed.
    Example:
            x = date_transform('23-03-2020','dd-mmm-yyyy','dd-mm-yy')"""
    
    logging.info(f"got input date is : {date}")
    logging.info(f"got input format is : {input_format}")
    logging.info(f"got expecting output format is : {output_format}")
    result = {}
    date_format_mapping = {'dd-mm-yyyy':'%d-%m-%Y','dd-mm-yy':'%d-%m-%y'}
    try:
        input_format_ = date_format_mapping[input_format]
        output_format_ = date_format_mapping[output_format]
    except Exception:
        input_format_ = '%d-%m-%Y'
        output_format_ = '%d-%m-%Y'
        
    try:
        date_series = pd.Series(date)
        logging.info(f"got series is : {date_series}")
    except Exception as e:
        logging.error("cannnot convert given input to pandas series")
        logging.error(e)
        result['flag'] = False
        result['data'] = {'reason':'cannnot convert given input to pandas series','error_msg':str(e)}
        
    try:
        try:
            converted_date = pd.to_datetime(date_series, format=input_format_, errors='coerce').dt.strftime(output_format_)
        except Exception:
            converted_date = pd.to_datetime(date_series, format=input_format_,errors='coerce',utc=True).dt.strftime(output_format_)

        logging.info(f"Got converted date is : {converted_date}")
        result['flag'] = True
        result['data'] = {"value": converted_date[0]}
        
    except Exception as e:
        logging.info("Failed while Converting date into given format")
        logging.info(e)
        result['flag'] = False
        result['data'] = {'reason':'cannnot convert given date to required format','error_msg':str(e)}
    
    return result

def get_data(tenant_id, database, table, case_id, case_id_based=True, view='records'):
    """give the data from database
    Args:
        
    Returns:
        result (dict)
        result is a dict having keys flag, data.Depending on the flag the data changes.
        if flag is True: data contains the key value and value is the data.
        if flag is False: data contains the error_msg say why its failed.
    Example:
            x = get_data('invesco.acelive.ai','extraction','ocr','INV4D15EFC')"""
    result = {}
    db_config['tenant_id'] = tenant_id

    db = DB(database, **db_config)
    try:
        if case_id_based:
            query = f"SELECT * from `{table}` WHERE `case_id` = '{case_id}'"
            try:
                df = db.execute(query)

            except Exception as e:
                logging.info(f"Error occured with Exception {e}")



                df = db.execute_(query)
            table_data = df.to_dict(orient= view)
            result['flag'] = True
            result['data'] = {"value":table_data}
            
        else:
            query = f"SELECT * from `{table}`"
            df = db.execute(query)
            if not df.empty:
                table_data = df.to_dict(orient = view)
            else:
                table_data = {}
            result['flag'] = True
            result['data'] = {"value":table_data}
    except Exception as e:
        logging.error("Failed in getting tables data from database")
        logging.error(e)
        result['flag'] = 'False'
        result['data'] = {'reason':'Failed in getting tables data from database','error_msg':str(e)}
    return result

def save_data(tenant_id, database, table, data, case_id, case_id_based=True):
    """Util for saving the data into database
    
    Args:
        tenant_id (str) -> the tenant name for which we have to take the database from. ex.invesco.acelive.ai
        database (str) -> database name. ex.extraction
        table (str) -> table name. ex.ocr
        case_id_based (bool) -> says whether we have to bring in all the data or only the data for a case_id.
        case_id (str) -> case_id for which we have to bring the data from the table.
        data (dict) -> column_value map or a record in the database.
    Returns:
        result (dict)
    Example:
        data1 = {"comments":"testing","assessable_value":1000}
        save_data(tenant_id='deloitte.acelive.ai', database='extraction', table='None', data=data1, case_id='DEL754C18D_test', case_id_based = True, view='records')"""
    logging.info(f"tenant_id got is : {tenant_id}")
    logging.info(f"database got is : {database}")
    logging.info(f"table name got is : {table}")
    logging.info(f"data got is : {data}")
    logging.info(f"case_id got is : {case_id}")
    result = {}
    
    if case_id_based:
        logging.info("data to save is case_id based data.")
        try:
            db_config['tenant_id'] = tenant_id
            connected_db = DB(database, **db_config) # only in ocr or process_queue we are updating
            connected_db.update(table, update=data, where={'case_id':case_id})
        except Exception as e:
            logging.error("Cannot update the database")
            logging.error(e)
            result["flag"]=False,
            result['data'] = {"reason":f"Cannot update the database, error_msg:{str(e)}"}
            return result
        result['flag']=True
        result['data']= data
        return result

    else:
        logging.info("data to save is master based data.")
        try:
            db_config['tenant_id'] = tenant_id
            connected_db = DB(database, **db_config) # only in ocr or process_queue we are updating
            logging.info('************** have to develop due to where clause condition not getting from data *******')
            connected_db.update(table, update=data, where={'case_id':case_id})
        except Exception as e:
            logging.error("Cannot update the database")
            logging.error(e)
            result['flag']=False
            result['data'] = {"reason":f"Cannot update the database, error_msg:{str(e)}"}
        
        result['flag']=True
        result['data']= data  
        return result


@app.route('/index', methods=['POST', 'GET'])
def index():
    return ('Hello world')


@app.route('/get_data', methods=['POST', 'GET'])
def get_data_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("failed to start ram and time calc")
    params = request.json
    case_id = params.get('case_id', None)
    tenant_id = params.get('tenant_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='get_data_route',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        database = params.get('database', None)
        table = params.get('table', None)
        case_id_based = params.get('case_id_based', True)
        view = params.get('view', 'records')

        if case_id_based == "False":
            case_id_based = False
        else:
            case_id_based = True
        result = get_data(tenant_id, database, table, case_id, case_id_based, view)

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("failed to calc end of ram and time")
            logging.exception("ram Calc went wrong")
            memory_consumed = None
            time_consumed = None

        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(result)

@app.route('/save_data', methods=['POST', 'GET'])
def save_data_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to  start ram and time calc")

    params = request.json
    tenant_id = params.get('tenant_id', None)
    case_id = params.get('case_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='save_data_route',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
    
        database = params.get('database', None)
        table = params.get('table', None)
        data = params.get('data', None)
        
        case_id_based = bool(params.get('case_id_based', True))
        result = save_data(tenant_id, database, table, data, case_id, case_id_based)

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("Failed To calc end of ram and time")
            logging.exception("ram calc Went wrong")
            memory_consumed = None
            time_consumed = None
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(result)

@app.route('/partial_match', methods=['POST', 'GET'])
def partial_match_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to Start ram and time calc")
    params = request.json
    case_id = params.get('case_id', None)
    tenant_id = params.get('tenant_id', None)
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='partial_match_route',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        input_string = params.get('input_string', None)
        matchable_strings = params.get('matchable_strings', [])
        result = partial_match(input_string, matchable_strings, threshold=75)

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went Wrong")
            memory_consumed = None
            time_consumed = None

        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(result)

@app.route('/date_transform', methods=['POST', 'GET'])
def date_transform_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram and time Calc")
    params = request.json
    tenant_id = params.get('tenant_id', None)
    case_id = params.get('case_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='date_transform_route',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        date = params.get('date', None)
        result = date_transform(date, input_format='dd-mm-yyyy', output_format='dd-mm-yyyy')

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("Failed to calc end of ram And time")
            logging.exception(" ram  calc went wrong")
            memory_consumed = None
            time_consumed = None
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}") 
        return jsonify(result)


@app.route('/assign', methods=['POST', 'GET'])
def assign_route():

    params = request.json    
    key_value_data = params.get('assign_table_data', None)
    key_to_assign = params.get('assign_column', None)
    value_to_assign = params.get('assign_value', None)
    key_value_data[key_to_assign] = value_to_assign

    return jsonify(key_value_data)
    















class Blockly(object):
    def __init__(self):
        self.name = "Blockly"
        self.method_string = ""
        self.retun_var="return_data"

    def function_builder(self,method_string,return_var="return_data"):
        self.method_string=method_string
        self.retun_var=return_var

        def fun():
            try:
                
                exec(self.method_string,globals(),locals())
                logging.info(f"####### local Vars: {locals()}")
                logging.info(f"####### self vars: {locals()['self']}")
                logging.info(f"####### test Vars: {locals()['test']}")
                return_data=locals()[self.retun_var]

                return True,return_data
            except Exception as e:
                logging.info("###### Error in executing Python Code")
                logging.exception(e)
                return False,str(e)

        return fun



def print_globals_types():
    for key in globals().keys():
        logging.info(f"######### Key: {key} and type: {type(globals()[key])}")


def function_builder(method_string,return_var="return_data"):
    
    

    def fun():
        try:
            
            
            logging.info(f"####### Function builder calling: {method_string}")
            
            exec(method_string,globals(),globals())
            logging.info(f"####### global keys : {globals().keys()}")

            
            return_dict = {}
            return_list = return_var.split(",")

            for param in return_list:
                return_dict[param] = globals().get(param,"")

            return True,return_dict
        except Exception as e:
            logging.info("###### Error in executing Python Code")
            logging.exception(e)
            return False,str(e)

    return fun



@app.route('/execute_business_rules',methods=['POST','GET'])
def execute_business_rules():
    try:
        memory_before, start_time = start_resource_measurement()
    except Exception:
        logging.warning("Failed to start ram and time calc")

    data = request.json
    case_id, tenant_id = data.get('case_id', None), data.get("tenant_id", None)
    trace_id = case_id or data.get('rule_id', None)

    attr = create_zipkin_attrs(trace_id, tenant_id)

    with zipkin_span(
            service_name='business_rules_api',
            span_name='execute_business_rules',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):

        message, _, db_config['tenant_id'] = {'flag': False}, True, tenant_id
        _, _ = data.get('link_type', "rule"), data.get('rule_name', "")
        

        try:
            memory_after, time_consumed, memory_consumed = end_resource_measurement(memory_before, start_time)
            log_resource_usage(memory_after, memory_consumed, time_consumed)
        except Exception:
            logging.warning("Failed to calc End of ram and time")

        return jsonify(message)


def start_resource_measurement():
    memory_before = measure_memory_usage()
    start_time = tt()
    return memory_before, start_time


def create_zipkin_attrs(trace_id, tenant_id):
    return ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )


def handle_rule_execution(rule_type, data):
    if rule_type == "rule":
        return execute_single_rule(data)
    elif rule_type == "chain":
        return execute_rule_chain(data)
    return "return_data"


def execute_single_rule(data):
    try:
        string_python = data.get('rule', {}).get('python', "")
        return_param = data.get('return_params', "return_data")
        logging.info(f"### Globals before execution: {globals().keys()}")
        logging.info(f"### Locals before execution: {locals().keys()}")
        return_data = test_business_rule(string_python, return_param)
        message = jsonify(return_data)
    except Exception as e:
        logging.error("Error in Executing the given Rule")
        logging.exception(e)
        message = {"flag": False, "message": "Error in executing the rule"}
    return message


def execute_rule_chain(data):
    try:
        rule_seq_list = data.get("group", [])
        if not rule_seq_list:
            return {"flag": False, "message": "Empty Rule list"}

        rule_seq_list.sort(key=lambda x: x['sequence'])
        logging.info(f"### Rule sequenced: {rule_seq_list}")
        return_data = "return_data"

        for rule in rule_seq_list:
            logging.info(f"### Executing rule id {rule['rule_id']}")
            fetch_code, rule = get_the_rule_from_db(rule['rule_id'])
            if not fetch_code:
                return rule
            execute_code, return_data = test_business_rule(rule, return_data)
            if not execute_code:
                return return_data

        return {'flag': True, 'data': return_data}
    except Exception as e:
        logging.error("Error in Executing the rule chain")
        logging.exception(e)
        return {"flag": False, "message": "Error in Executing the rule chain"}


def end_resource_measurement(memory_before, start_time):
    memory_after = measure_memory_usage()
    memory_consumed = (memory_after - memory_before) / (1024 * 1024 * 1024)
    end_time = tt()
    time_consumed = str(round(end_time - start_time, 3))
    memory_consumed = f"{memory_consumed:.10f}"
    return memory_after, time_consumed, memory_consumed


def log_resource_usage(memory_after, memory_consumed, time_consumed):
    logging.info(f"checkpoint memory_after - {memory_after}, memory_consumed - {memory_consumed}, time_consumed - {time_consumed}")


@app.route('/execute_camunda_business_rules',methods=['POST','GET'])
def execute_camunda_business_rules():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed   to start ram and time calc")
        

    data = request.json
    case_id = data.get('case_id', None)
    tenant_id = data.get("tenant_id",None)

    if case_id is None:
        trace_id = data.get('rule_id',"")
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='execute_camunda_business_rules',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        db_config['tenant_id']=tenant_id

        return_data="return_data"
        rule_id = data.get('rule_id',"")
        try:
                return_param = data.get('return_param',"return_data")

                

                fetch_code,rule = get_the_rule_from_db(rule_id)
                if fetch_code:
                    return_data = test_business_rule(rule,return_param)
                    logging.info(f"########### Return data: {return_data}")
                    

                else:
                    return_data = {'flag':True,'message':'Error in fetcing rule from db'}
                    

        except Exception as e:
                logging.info("######## Error in Executing the given  Rule")
                logging.exception(e)
                return_data = {"flag":False,"message":"Error in executing the Given rule"}
                
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("Failed to calc end Of ram and time")
            logging.exception("ram calc went  wrong")
            memory_consumed = None
            time_consumed = None
            
        
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)
       

def get_the_rule_from_db(rule_id):
    business_db = DB("business_rules",**db_config)


    try:  

        fetch_query = f"select python_code from rule_base where rule_id='{rule_id}'"

        rule_list=business_db.execute_(fetch_query)

        if len(rule_list['python_code'])>0:
            return True,rule_list['python_code'][0]

        else:
            return False,"No Rule for given Rule ID"

    except Exception:

        return False,"Error in fetching rule from DB"



def execute_rule_chain(rule_chain):
    try:
        for rule in rule_chain:
            logging.info(f"######### Executing rule id {rule['rule_id']}")

            fetch_code,rule = get_the_rule_from_db(rule['rule_id'])
            if fetch_code:
                execute_code,return_data=test_business_rule(rule,"return_data")
                if not execute_code:
                    return return_data

            else:
                return rule

        return "RAN all rules"


    except Exception as e:
        logging.info("######## Error in Executing the  given Rule CHAIN LOOP")
        logging.exception(e)
        return jsonify({"flag":False,"message":"Error in executing the given rule chain LOOP"})


def test_business_rule(string_python,return_var='return_data'):

    return_message=""
    return_code = True


    if string_python != "" and "rm -rf" not in string_python:

        logging.info(f"######### The given code is : {string_python}")

        exec_code = function_builder(string_python,return_var)

        logging.info("##### Calling Python Business Rules")

        return_code,return_message = exec_code()


    else:
        message = "The python block is empty or running a excluded method like rm -rf"
        return_data = {"flag":False,"message":message}
        return return_data

    return_data = {"flag":return_code,"data":return_message}

    logging.info(f"############## Returning data from test business rule: {return_data}")

    return return_data


@app.route('/rule_builder_data',methods=['POST','GET'])
def rule_builder_data():
    def log_and_return(message, flag=False):
        logging.info(message)
        return jsonify({"flag": flag, "message": message})

    def process_time_and_memory(start_time, memory_before):
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after}, memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time - start_time, 3))
        except Exception:
            logging.warning("Failed to calculate RAM and time at the end")
            logging.exception("RAM calculation went wrong")
            return None, None
        return memory_consumed, time_consumed

    def fetch_rule_data(rule_id, business_db):
        try:
            fetch_query = f"select * from rule_base where rule_id = '{rule_id}'"
            rule_dict = business_db.execute(fetch_query).to_dict(orient="records")
            if rule_dict:
                rule_data = process_rule_data(rule_dict[0])
                logging.info(f"Fetched rule for {rule_id}: {rule_data}")
                return {"flag": True, "data": rule_data}
            else:
                return {"flag": True, "data": {}}
        except Exception as e:
            logging.exception(e)
            return {"flag": False, "message": "Error fetching the rule from DB"}

    def process_rule_data(rule_data):
        rule_data['rule'] = {
            'xml': rule_data.pop('xml'),
            'javascript': rule_data.pop('javascript_code'),
            'python': rule_data.pop('python_code')
        }
        return rule_data

    def handle_save_edit(flag, rule_base_table_dict, business_db, rule_id):
        try:
            if flag == 'save':
                return save_rule(rule_base_table_dict, business_db, rule_id)
            elif flag == 'edit':
                return edit_rule(rule_base_table_dict, business_db, rule_id)
        except Exception as e:
            logging.exception(e)
            return log_and_return(f"Error {flag}ing the rule to DB")
        return None

    def save_rule(rule_base_table_dict, business_db, rule_id):
        rule_base_table_dict['rule_id'] = rule_id
        if not business_db.insert_dict(table="rule_base", data=rule_base_table_dict):
            return log_and_return("Duplicate Rule ID or Error saving the rule to DB")
        return None

    def edit_rule(rule_base_table_dict, business_db, rule_id):
        business_db.update(table="rule_base", update=rule_base_table_dict, where={"rule_id": rule_id})
        return None

    def initialize_timing_and_memory():
        try:
            memory_before = measure_memory_usage()
            start_time = tt()
            return memory_before, start_time
        except Exception:
            logging.warning("Failed to start RAM and time calculation")
            return None, None

    def validate_input(data):
        tenant_id = data.get('tenant_id')
        rule_id = data.get('rule_id', "")
        if not tenant_id or not rule_id:
            return log_and_return("Please send valid request data"), None
        return None, (tenant_id, rule_id)

    def execute_rule(data):
        try:
            string_python = data.get('rule', {}).get('python', "")
            return_param = data.get('return_param', "return_data")
            return test_business_rule(string_python, return_param)
        except Exception as e:
            logging.exception(e)
            return log_and_return("Error executing the rule")

    # Main function logic starts here
    memory_before, start_time = initialize_timing_and_memory()
    data = request.json

    error_response, validation = validate_input(data)
    if error_response:
        return error_response

    tenant_id, rule_id = validation
    trace_id = data.get('case_id') or rule_id
    attr = ZipkinAttrs(trace_id=trace_id, span_id=generate_random_64bit_string(), parent_span_id=None, flags=None, is_sampled=False, tenant_id=tenant_id)

    with zipkin_span(service_name='business_rules_api', span_name='rule_builder_data', transport_handler=http_transport, zipkin_attrs=attr, port=5010, sample_rate=0.5):
        username, flag, rule_name = data.get('user', ""), data.get('flag', ""), data.get('rule_name', "")
        if not username or not flag:
            return log_and_return("Invalid user or flag")

        rule_base_table_dict = {
            'rule_name': rule_name,
            'description': data.get('description', ""),
            'xml': data.get('rule', {}).get('xml', ""),
            'python_code': data.get('rule', {}).get('python', ""),
            'javascript_code': data.get('rule', {}).get('javascript', ""),
            'last_modified_by': username
        }

        db_config['tenant_id'] = tenant_id
        business_db = DB("business_rules", **db_config)

        if flag in ['save', 'edit']:
            result = handle_save_edit(flag, rule_base_table_dict, business_db, rule_id)
            if result:
                return result
        elif flag == 'fetch':
            return jsonify(fetch_rule_data(rule_id, business_db))
        elif flag == 'execute':
            return_data = execute_rule(data)

    memory_consumed, time_consumed = process_time_and_memory(start_time, memory_before)
    logging.info(f"BR Time and RAM checkpoint: Time consumed: {time_consumed}, RAM consumed: {memory_consumed}")

    return jsonify(return_data)


def insert_or_update_chain_linker(database,table,data_dict):

    try:
        logging.info("########### Tyring to insert the data")

        insert_status = database.insert_dict(table=table,data=data_dict)

        if insert_status == False or insert_status is None:
            logging.info("############# Failed to insert ")
            logging.info("############# Trying to Update the Data")

            

            







            data_dict.pop('created_by')



        return True

    except Exception as e:
        logging.info("############ Error in Insert/Update to Chain linker table")
        logging.exception(e)
        return False


def chain_linker_db_logic(request_data,database):

    

    username = request_data.get('user',"")
    
    group_id = request_data.get('group_id',"")
    
    group_list = request_data.get('group',[])

    chain_link_data_dict = {'group_id': group_id, 'last_modified_by': username}

    try:
        logging.info(f"########## Trying to clear for group_id {group_id}")
        delete_group_query = f"delete from chain_linker where group_id='{group_id}'"
        database.execute(delete_group_query)

    except Exception as e:
        logging.info("############## Error in deleting the links for group")
        logging.exception(e)


    
    if group_list:
        for link in group_list:
            chain_link_data_dict['rule_id']=link['rule_id']
            chain_link_data_dict['sequence']=link['sequence']
            chain_link_data_dict['link_type']=link['link_type']
            chain_link_data_dict['created_by']=username
            save_check = insert_or_update_chain_linker(database=database,table="chain_linker",data_dict=chain_link_data_dict)

            if not save_check:
                return jsonify({'flag':False,'message':"Group List/Data is Empty"})


    else:
        return jsonify({'flag':False,'message':"Group List/Data is Empty"})

    return jsonify({'flag':True,'message':"Successfully Saved to Chain Linker Table"})


def check_if_id_exists(column,value,database,table):

    check_query =  f"select count(*) from {table} where {column}='{value}' "

    try:
        check_df = database.execute_(check_query)['count']

        if len(check_df)>0:
            return True
        else:
            return False

    except Exception as e:
        logging.info(f"########## Error while check {column} is existence")
        logging.exception(e)
        return None
        


@app.route('/get_rules_data',methods=['GET','POST'])
def get_routes():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram And time calc")
        
    data = request.json
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='get_routes',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        db_config['tenant_id'] = tenant_id

        business_rules_db = DB('business_rules',**db_config)

        try:
            fetch_query = "select rule_id,rule_name,description as rule_description from rule_base"
            rule_list = business_rules_db.execute_(fetch_query).to_dict(orient='records')

            return_data = {'flag':True,'data':rule_list}
            

        except Exception as e:
            logging.info("######## Error in fetching all rules")
            logging.exception(e)
            return_data = {"flag":False,"message":"Error in fetching rules"}
            
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("Failed to   calc end of ram and time")
            logging.exception("  ram calc went   wrong")
            memory_consumed = None
            time_consumed = None
            
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)


    
@app.route('/get_rule_from_id',methods=['GET','POST'])
def get_rule_from_id():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram and Time calc")
        

    data = request.json
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='get_rule_from_id',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        rule_id = data.get('rule_id',"")
        db_config['tenant_id'] = tenant_id
        business_rules_db = DB('business_rules',**db_config)

        try:
            fetch_query = f"select rule_id,rule_name,xml,description as rule_description from rule_base where rule_id='{rule_id}'"
            rule_list = business_rules_db.execute_(fetch_query).to_dict(orient='records')

            
            return_data = {'flag':True,'data':rule_list}

        except Exception as e:
            logging.info("######## Error in fetching all rules")
            logging.exception(e)
            return_data = {"flag":False,"message":"Error in fetching rules"}
            
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("Failed to calc end of ram   and time")
            logging.exception("Ram   calc went wrong")
            memory_consumed = None
            time_consumed = None
            

        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)


def block_db(database,statement):


    

    db_obj = DB(database, **db_config)

    try:

        return_data2=db_obj.execute_(statement).to_dict(orient='records')

        
        return return_data2


    except Exception as e:
        logging.info("######### Error in Running test function")
        logging.info(e)

    
def block_get_var(var_name):

    return globals().get(var_name,"")


@app.route('/check_function_builder',methods=['GET','POST'])
def check_function_builder():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed TO start ram and time calc")
        

    data = request.json
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='check_function_builder',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        test_function = data.get('function',"logging.info('Hello World')")

        exec_code = function_builder(test_function)

        exec_code()

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("Failed to calc end of ram and   time")
            logging.exception("ram calc Went   wrong")
            memory_consumed = None
            time_consumed = None
            
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify({"flag":True,"message":"message"})


def get_data_sources(business_rules_db, case_id, column_name, master_data_columns=None, master=False):
    """Helper to get all the required table data for the business rules to apply"""

    if master_data_columns is None:
        master_data_columns = {}

    def fetch_data(db, table, case_id, columns_list=None):
        query = f"SELECT {columns_list or '*'} from `{table}`"
        if not master:
            query += " WHERE case_id = %s"
            df = db.execute_(query, params=[case_id])
        else:
            df = db.execute_(query)

        return df.to_dict(orient='records') if not df.empty else {}

    def process_sources(db_config, sources):
        data = {}
        for database, tables in sources.items():
            db = DB(database, **db_config)
            for table in tables:
                columns_list = ', '.join(master_data_columns.get(table, ['*'])) if master and master_data_columns else None
                data[table] = fetch_data(db, table, case_id, columns_list)
        return data

    # Retrieve data sources
    data_sources = business_rules_db.execute_("SELECT * from `data_sources`")
    sources = json.loads(list(data_sources[column_name])[0]) if data_sources[column_name] else {}
    logging.info(f"sources: {sources}")

    if not sources:
        return {}, sources  # Early return if no sources

    # Process and return data
    data = process_sources(db_config, sources)

    return data, sources


def function_check(tenant_id, case_id, rule_list, data_tables, update_table_sources, return_vars):
    if len(rule_list)>0:
        BR = BusinessRules(case_id, rule_list, data_tables)
        BR.tenant_id = tenant_id
        BR.return_vars=return_vars

        return_data = BR.evaluate_rule(rule_list)
            
        if BR.changed_fields:
            updates = BR.changed_fields
            BR.update_tables(updates, data_tables, update_table_sources)
        logging.info(f"###### Return Data {return_data}")
        
    else:
        logging.info(f"############ Rules are empty, please check {rule_list}")
        

@app.route('/run_business_rule', methods=['POST', 'GET'])
def run_business_rule():
    try:
        memory_before = measure_memory_usage()
    except Exception:
        logging.warning("Failed to start ram and time calc")

    data = request.json
    logging.info(f"######## Running business with the data: {data}")

    case_id, rule_id, tenant_id, user, session_id = extract_initial_data(data)
    trace_id = rule_id if case_id is None else case_id

    if not user or not session_id:
        user, session_id = extract_ui_data(data)

    attr = create_zipkin_attributes(trace_id, tenant_id)

    with zipkin_span(
        service_name='business_rules_api',
        span_name='run_business_rule',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
        business_rules_db = DB('business_rules',**db_config)
        # Extract required data
        return_vars, field_changes, master_data_columns, master_data_require = extract_additional_data(data)

        # Validate case_id, tenant_id, and rule_id
        if not validate_input(case_id, tenant_id, rule_id):
            return jsonify({"flag": False, "message": "Please provide valid case_id, tenant_id and rule_id"})

        # Fetch data from sources
        data_tables, update_table_sources = process_data_sources(business_rules_db, case_id, master_data_require, master_data_columns)

        # Execute business rules and return data
        response_data = execute_business_rules(data_tables, case_id, rule_id, return_vars, field_changes, update_table_sources)

        # Log memory usage and time
        log_memory_usage(memory_before)

        return jsonify(response_data)

def extract_initial_data(data):
    case_id = data.get("case_id", None)
    rule_id = data.get("rule_id", "")
    tenant_id = data.get("tenant_id", None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    return case_id, rule_id, tenant_id, user, session_id

def extract_ui_data(data):
    ui_data = data.get('ui_data', {'user': None, 'session_id': None})
    return ui_data['user'], ui_data['session_id']

def create_zipkin_attributes(trace_id, tenant_id):
    return ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

def extract_additional_data(data):
    return_vars = data.get("return_vars", "")
    field_changes = data.get('ui_data', {}).get('field_changes', [])
    master_data_require = data.get('master_data_require', 'False')
    master_data_columns = data.get('master_data_columns', {})
    if master_data_columns:
        master_data_columns = ast.literal_eval(master_data_columns)
    return return_vars, field_changes, master_data_columns, master_data_require

def validate_input(case_id, tenant_id, rule_id):
    return not (case_id == "" or tenant_id == "" or rule_id == "")

def process_data_sources(business_rules_db, case_id, master_data_require, master_data_columns):
    case_id_data_tables, case_id_sources = get_data_sources(business_rules_db, case_id, 'case_id_based')

    if master_data_require == 'True':
        master_data_tables, master_data_sources = get_data_sources(
            business_rules_db, case_id, 'master', master_data_columns, master=True)
    else:
        master_data_tables, master_data_sources = {}, {}

    logging.info(f"case_id_sources: {case_id_sources}")
    logging.info(f"master_data_tables: {master_data_tables}")
    logging.info(f"master_data_sources: {master_data_sources}")

    data_tables = {**case_id_data_tables, **master_data_tables}
    update_table_sources = merge_data_sources(case_id_sources, master_data_sources)

    return data_tables, update_table_sources

def merge_data_sources(case_id_sources, master_data_sources):
    update_table_sources = {}

    for key in case_id_sources:
        update_table_sources[key] = case_id_sources[key] + master_data_sources.get(key, [])
    
    for key in master_data_sources:
        if key not in update_table_sources:
            update_table_sources[key] = master_data_sources[key]

    logging.info(f"update_table_sources: {update_table_sources}")
    return update_table_sources

def execute_business_rules(data_tables, case_id, rule_id, return_vars, field_changes, update_table_sources):
    try:
        rule_id = ast.literal_eval(rule_id) if return_vars == '' else rule_id
    except Exception:
        pass
    business_rules_db = DB('business_rules',**db_config)
    rule_list = fetch_rule_list(business_rules_db, rule_id)
    response_data = evaluate_rules(rule_list, case_id, data_tables, field_changes, return_vars, update_table_sources)

    return response_data

def fetch_rule_list(business_rules_db, rule_id):
    if isinstance(rule_id, list) and len(rule_id) == 1:
        rule_id = rule_id[0]
        query = f"select python_code from rule_base where rule_id='{rule_id}'"
    else:
        rule_id = tuple(rule_id) if isinstance(rule_id, list) else rule_id
        query = f"select python_code from rule_base where rule_id in {rule_id}"
    
    return business_rules_db.execute_(query)

def evaluate_rules(rule_list, case_id, data_tables, field_changes, return_vars, update_table_sources):
    response_data = {}
    data = request.json
    for i, python_code in enumerate(rule_list['python_code']):
        BR = BusinessRules(case_id, python_code, data_tables)
        BR.tenant_id = data.get('tenant_id')
        BR.return_vars = return_vars
        BR.field_changes = field_changes

        return_data = BR.evaluate_rule(python_code)

        if BR.changed_fields:
            BR.update_tables(BR.changed_fields, data_tables, update_table_sources)
            logging.info(f"updates: {BR.changed_fields}")
        response_data = {"flag": True, "data": return_data}
        
    return response_data

def log_memory_usage(memory_before):
    try:
        memory_after = measure_memory_usage()
        memory_consumed = (memory_after - memory_before) / (1024 * 1024 * 1024)
        memory_consumed = f"{memory_consumed:.10f}"
        logging.info(f"Memory consumed: {memory_consumed}")
    except Exception:
        logging.warning("Failed to calculate memory and time")


@app.route('/get_ui_rules', methods=['POST', 'GET'])
def get_ui_rules():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram  and time calc")
        
    data = request.json
    case_id = data.get('case_id', None)
    tenant_id = data.get("tenant_id", None)
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='business_rules_api',
            span_name='get_ui_rules',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):

    

        db_config['tenant_id'] =tenant_id

        business_rules_db = DB('business_rules', **db_config)

        try:

            
            fetch_query = "SELECT RULE_ID, JAVASCRIPT_CODE  FROM RULE_BASE"
            rule_list = business_rules_db.execute_(fetch_query).to_dict(orient='records')

            rules_dict = {row['RULE_ID']: row['JAVASCRIPT_CODE'] for row in rule_list}
            return_data = {'flag':True,'data':rules_dict}

                   

        except Exception as e:
            logging.execption("####### Error in fetching UI business rules",e)
            return_data = {'flag':False,'message':'Error in fetchign UI business rules'}
            
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except Exception:
            logging.warning("failed to Calc end of ram and time")
            logging.exception("Ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)

@app.route('/business_rules_api_health_check', methods=['POST', 'GET'])
def business_rules_api_health_check():
    return jsonify({'flag':True})
