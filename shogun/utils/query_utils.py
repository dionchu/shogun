PANDAS_MAX_VARIABLE_NUMBER = 998

def group_into_chunks(items, chunk_size=PANDAS_MAX_VARIABLE_NUMBER):
    items = list(items)
    return [items[x:x+chunk_size]
            for x in range(0, len(items), chunk_size)]
