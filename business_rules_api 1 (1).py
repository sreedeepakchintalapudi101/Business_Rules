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
                rule_data = rule_dict[0]
                rule_data['rule'] = {
                    'xml': rule_data.pop('xml'),
                    'javascript': rule_data.pop('javascript_code'),
                    'python': rule_data.pop('python_code')
                }
                logging.info(f"Fetched rule for {rule_id}: {rule_data}")
                return {"flag": True, "data": rule_data}
            else:
                return {"flag": True, "data": {}}
        except Exception as e:
            logging.exception(e)
            return {"flag": False, "message": "Error fetching the rule from DB"}

    def handle_save_edit(flag, rule_base_table_dict, business_db, rule_id):
        try:
            if flag == 'save':
                rule_base_table_dict['rule_id'] = rule_id
                if not business_db.insert_dict(table="rule_base", data=rule_base_table_dict):
                    return log_and_return("Duplicate Rule ID or Error saving the rule to DB")
            elif flag == 'edit':
                business_db.update(table="rule_base", update=rule_base_table_dict, where={"rule_id": rule_id})
        except Exception as e:
            logging.exception(e)
            return log_and_return(f"Error {flag}ing the rule to DB")
        return None

    def initialize_timing_and_memory():
        try:
            memory_before = measure_memory_usage()
            start_time = tt()
            return memory_before, start_time
        except Exception:
            logging.warning("Failed to start RAM and time calculation")
            return None, None

    memory_before, start_time = initialize_timing_and_memory()
    data = request.json
    case_id, tenant_id, rule_id = data.get('case_id'), data.get('tenant_id'), data.get('rule_id', "")

    if not all([tenant_id, rule_id]):
        return log_and_return("Please send valid request data")

    trace_id = case_id or rule_id
    attr = ZipkinAttrs(trace_id=trace_id, span_id=generate_random_64bit_string(), parent_span_id=None, flags=None, is_sampled=False, tenant_id=tenant_id)

    with zipkin_span(service_name='business_rules_api', span_name='rule_builder_data', transport_handler=http_transport, zipkin_attrs=attr, port=5010, sample_rate=0.5):
        username, flag, rule_name = data.get('user', ""), data.get('flag', ""), data.get('rule_name', "")
        if not all([username, flag]):
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
            try:
                string_python = data.get('rule', {}).get('python', "")
                return_param = data.get('return_param', "return_data")
                return_data = test_business_rule(string_python, return_param)
            except Exception as e:
                logging.exception(e)
                return log_and_return("Error executing the rule")

    memory_consumed, time_consumed = process_time_and_memory(start_time, memory_before)
    logging.info(f"BR Time and RAM checkpoint: Time consumed: {time_consumed}, RAM consumed: {memory_consumed}")
    
    return jsonify(return_data)
