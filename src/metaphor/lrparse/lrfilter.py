
def aggregate_from_filter(filter_str):
    aggregate = {}
    fields = filter_str.split(',')
    for fil in fields:
        if '=' in fil:
            name, value = fil.split('=')
            if '"' in value or "'" in value:
                value = value.strip('"').strip("'")
            else:
                value = value.strip('"').strip("'")
                value = int(value)
            aggregate[name] = value
    return {'$match': aggregate}
