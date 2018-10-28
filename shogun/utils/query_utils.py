PANDAS_MAX_VARIABLE_NUMBER = 998

def group_into_chunks(items, chunk_size=PANDAS_MAX_VARIABLE_NUMBER):
    items = list(items)
    return [items[x:x+chunk_size]
            for x in range(0, len(items), chunk_size)]

def query_df(df, eq_conditions = None, ne_conditions = None):
    """A wrapper function to simplify DataFrame queries

    Parameters
    ----------
    df: DataFrame object
        The DataFrame object being queried
    conditions: Python dictionary
        A dictionary mapping key values: column names,
        to values: target values
    """
    count = 0
    query = ""
    if eq_conditions:
        for k, v in eq_conditions.items():
            if count == 0:
                query += '(df[\'' + k + '\'] == \'' + v + '\')'
            else:
                query += ' & ' + '(df[\'' + k + '\'] == \'' + v + '\')'
            count += 1
    if ne_conditions:
        for k, v in ne_conditions.items():
            if count == 0:
                query += '(df[\'' + k + '\'] != \'' + v + '\')'
            else:
                query += ' & ' + '(df[\'' + k + '\'] != \'' + v + '\')'
            count += 1
    return eval('df[' + query + ']')
