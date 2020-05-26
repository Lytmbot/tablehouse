# tablehouse

Autocomplete is nice. 

clone, then use with:

    import sys
    sys.path.append('/path/to/tablehouse')
    import tablehouse
    
    # if clickhouse server is running locally
    ch1_host = 'http://127.0.0.1:8123'
    ch1 = tablehouse.get_tables(ch1_host)
    
    # access database/table structure and pull data as
    df = ch1.database_name.table_name.pull_data()
