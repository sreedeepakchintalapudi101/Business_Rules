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
