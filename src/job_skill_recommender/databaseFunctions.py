import mysql.connector  # To connect and send data to DB
import re  # Regex operations


def mySqlDatabaseConnect(database, password, host='localhost', user='root'):
    '''Connects to mySQL and creates a cursor'''
    db = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    # Create db object needed to make Database operations
    return db


def mySqlCreateNewColumnsIfNotExist(df, datatable, db):
    '''
    Dynamic creation of columns if they do not exists in sql DB yet
    datatable - name of table to be referenced
    db - database object from mysql.connector library
    dataframe - dataframe with columns to check
    '''
    cursor = db.cursor(buffered=True)  # Create a cursor
    cols = [i for i in df.columns]  # Fetch All Column names
    # Prepare columns for a query;
    columnsToQuery = "`"+"`,`".join(cols)+"`"

    # Prepare a query
    query = f'SELECT * from {datatable} LIMIT 1;'

    cursor.execute(query)  # Execute
    databaseColumns = cursor.column_names  # Get ALL database column names

    # For every column in items
    counter = 0
    for item in cols:
        # Check if it is not already defined in the database
        if (item not in databaseColumns):
            # If it isn't - perform a query to add it
            cursor.execute(
                f"ALTER TABLE {datatable} ADD COLUMN `{item}` TINYINT;")
            counter += 1
    print(f'{counter} columns inserted')


def insertValuesIntoSqlDatabase(df, datatable, db):
    '''
    Inserts values in MySQL Database
    datatable - name of table to be referenced
    cursor - cursor object from mysql.connector library
    dataframe - dataframe with data to insert
    '''
    cursor = db.cursor(buffered=True)
    cols = [i for i in df.columns]  # Fetch All Column names
    columnsToQuery = "`"+"`,`".join(cols)+"`"

    # Make a tuple containing values
    for i in range(1, len(df)+1):
        tuples = [tuple(x) for x in df.iloc[i-1:i].to_numpy()]
        # Build a query. It inserts values to corresponding columns that were found on db
        query = re.sub(
            "\[|\]", "",   f"INSERT INTO {datatable} ({columnsToQuery}) VALUES {tuples} ;")
        # Try executing the query
        try:
            cursor.execute(query)
            db.commit()

        except Exception as e:
            print(e)
            print(
                'Error - something went wrong with mySQL db update.\n Check for duplicate id')
    print(f'{datatable} : {cursor.rowcount} records inserted')
