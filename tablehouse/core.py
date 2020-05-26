from pandahouse import read_clickhouse, to_clickhouse
import collections

class Table:
    """
    Represent a clickhouse table and all associated metadata.
    """

    def __init__(self, columns, db_name, tbl_name, host_address):
        self.columns = columns
        self.owning_db = db_name
        self.tbl_name = tbl_name
        self.host_address = host_address


    def get_column_names(self):        
        return [col_name for col_name in self.columns._fields]


    def describe_table(self):
        print(self.owning_db, '({})'.format(self.host_address), '\n  ', self.tbl_name)
        for col in self.columns:
            print('    ',col.col_name, col.data_type, col.codec)
            
            
    def pull_data(self, start, stop, conditions='', columns='*'):
        """
        Pull data from clickhouse table to pandas.DataFrame with condition: 'BETWEEN start AND stop'
        
        Args:
            start : DateTime or string YYYY-MM-DD hh:mm:ss
            stop : DateTime or string YYYY-MM-DD hh:mm:ss
            conditions (str, optional) : Additioanl conditions to apply to query. Default is to apply none: ''
                See <https://clickhouse.tech/docs/en/sql-reference/statements/select/where/>
            columns (optional): Columns to include in query. Default is to select all: '*'


        Returns:
            pandas.DataFrame: Reults of query executed on clickhouse table
        """
                
        # handle columns formatting
        if not isinstance(columns, str) or isinstance(columns, collections.Iterable):    
            if isinstance(columns, dict):
                cols_string = ', '.join([key + ' AS ' + columns[key] for key in columns])
            else:
                cols_string = ', '.join(columns)
        elif isinstance(columns, str):
            # insert directly assuming its formatted properly
            cols_string = columns    
        else:
            print('Unsuported type: ({})'.format(type(columns)))
            print('param: columns must be a string or iterable')
            raise ValueError
        
        query = """
            SELECT {} FROM {}.{} 
            WHERE Timestamp BETWEEN toDateTime('{}') AND toDateTime('{}') 
            {}
        """.format(cols_string, self.owning_db, self.tbl_name, start, stop, conditions)

        return read_clickhouse(
            query = query,
            connection = {'host': self.host_address, 'database': self.owning_db}
            )
    

class Column:
    """
    Represents a clickhouse column and all associated metadata
    """
    def __init__(self, col_name, data_type, codec=None):
        self.col_name = col_name
        self.data_type = data_type
        self.codec = codec
        
        
        
def get_tables(host_address):
    """
    Returns an obj that mirrors the database/tables/columns structure on the host.

    This is useful if you want auto complete to get database and table names.

    The lowest nested level: 
        database_name.table_name -> Table 
    property will be a Table obj made up of the owning database, table and columns metadata.

    Args:
        host_address (str): adress for clickhouse server

    Returns: 
        object: dynamically created object that nests database_names.table_names.Table as properties
    """
        
    df = read_clickhouse(
        query = 'SELECT database, table, name, type, compression_codec as codec FROM system.columns',
        connection = {
            'host': host_address,
            'database': 'system'
            }
        )

    db_dict = {}
    for db_name, df_dbs in df.groupby(['database']):   
        tbls_dict = {}
        for tbl_name, df_tbls in df_dbs.groupby(['table']):   
            cols=[]
            for _, row in df_tbls.iterrows():
                cols.append(
                    Column(
                        col_name = row['name'], 
                        data_type=row['type'],
                        codec = row['codec']
                    )
                )

            tbls_dict[tbl_name] = Table(
                columns=cols,
                db_name=db_name,
                tbl_name=tbl_name,
                host_address=host_address
            )

        db_dict[db_name] = type('tables', (object,), tbls_dict)

    return type('database', (object,), db_dict)
    

# from clickhouse_driver import Client
# import numpy as np
# client = Client(host_address)
# query = 'SELECT database, table, name, type, compression_codec as codec FROM system.columns'
# arr = np.array(client.execute(query))

# db_dict = {'host_address':host_address}
# for db_name in np.unique(arr[:,0]):

#     tbls_dict = {}
#     tbls = np.unique(arr[arr[:,0]==db_name, 1])

#     for tbl_name in tbls: 
#         cols=[]

#         for col in arr[(arr[:,0]==db_name) & (arr[:,1]==tbl_name)]:

#             cols.append(
#                 Column(
#                     col_name = col[2],
#                     data_type=col[3],
#                     codec = col[4]
#                 )
#             )

#         tbls_dict[tbl_name] = Table(
#             columns=cols,
#             db_name=db_name,
#             tbl_name=tbl_name
#         )

#     db_dict[db_name] = type('tables', (object,), tbls_dict)

# return type('server', (object,), db_dict)