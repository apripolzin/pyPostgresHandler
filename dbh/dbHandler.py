# -*- coding: utf-8 -*-

import psycopg2
from configparser import ConfigParser

class DbHandler:
    def __init__(self):
        try:
            self.connection = get_db_connection()
        except Exception as e:
            raise e

    def __del__(self):
        if self.connection:
            # print("Closing connection...")
            self.connection.commit()
            self.connection.close()

    def update_table_set_where(self, table, set={}, where={}):
        '''
        Api for database communication
        :param table: table_name
        :param set: dict what to set
        :param where: dict where to set
        :return: number of updated rows
        '''
        set_string = ''
        values = []
        i = 1

        for column in set:
            set_string += ' "{col}"=%s '.format(col=column)
            values.append(set[column])
            if i < len(set.keys()):
                set_string += ', '
            i += 1

        where_string = ''

        i = 1
        for column in where:
            where_string += ' "{col}"=%s '.format(col=column)
            values.append(where[column])
            if i < len(where.keys()):
                where_string += ' AND '
            i += 1

        command = "UPDATE {table_name} SET {set_string} WHERE {where_string};".format(table_name=table, set_string=set_string, where_string=where_string)

        try:
            cur = self.connection.cursor()
            cur.execute(command, values)
            rowcount = cur.rowcount
            self.connection.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.commit()
            cur.close()
            raise Exception(str(e))

        return rowcount

    def delete_from_where(self, table, columns_values={}):
        '''
        Api for database communication
        :param table: table_name
        :param columns_values: dict what rows to delete
        :return: deleted id
        '''
        str_columns = ''
        values = []

        i = 1
        for column in columns_values:
            column = '"' + column + '"'
            str_columns += column + '=%s '
            values.append(columns_values[column])
            if i < len(columns_values):
                str_columns += 'AND '
            i += 1

        command = '''
        DELETE FROM {table} WHERE {columns} RETURNING *;
        '''.format(table=table, columns=str_columns)
        id = None
        try:
            cur = self.connection.cursor()
            cur.execute(command, values)
            result = cur.fetchone()
            if result:
                id = result[0]
            self.connection.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.commit()
            cur.close()
            raise Exception(str(e))
        return id

    def insert_into(self, table='', columns_and_values={}):
        '''
        Api for database communication
        :param table: table_name
        :param columns_and_values: dict of column values to insert 
        :return: id of new row
        '''
        id_column = self.get_columns_by_table_name(table)[0]
        str_columns = ''
        str_values = ''
        values = []
        i = 1
        for column in columns_and_values:
            str_columns += '"' + column + '"'
            str_values += '%s'
            values.append(columns_and_values[column])
            if i < len(columns_and_values.keys()):
                str_columns += ', '
                str_values += ', '
            i += 1

        command = '''
        INSERT INTO {table_name}({str_columns}) VALUES({str_values}) RETURNING {primary_key};
        '''.format(table_name=table, str_columns=str_columns, str_values=str_values, primary_key=id_column)

        id = None
        try:
            cur = self.connection.cursor()
            cur.execute(command, values)
            id = cur.fetchone()[0]
            self.connection.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.commit()
            cur.close()
            raise Exception(str(e))
            return None
        return id


    def get_columns_by_table_name(self, table_name):
        '''
        Returns list of columns in database schema
        :param table_name: table_name
        :return: list of columns
        '''
        command = '''
        SELECT column_name FROM information_schema.columns where table_name = %s;      
        '''
        result = []
        try:
            cur = self.connection.cursor()
            cur.execute(command, (table_name, ))
            for col in cur.fetchall():
                result.append(col[0])
            self.connection.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.commit()
            cur.close()
            raise Exception(str(e))

        return result

    def getDataFromTable(self, table_name, columns='*', where={}, order_by_id=True):
        '''
        Returns table data by list of dicts
        :param table_name: table name
        :param columns: tuple of columns to extract of '*' string if extract all columns
        :param where: dict to set the filter key means column name and value - expecting value
        :return: list of dict
        '''

        if columns == '*':
            columns = self.get_columns_by_table_name(table_name)

        id_name = '"' + self.get_columns_by_table_name(table_name)[0] + '"'

        i = 0
        columns_str = ''
        for col in columns:
            col = '"'+col+'"'
            i += 1
            if i >= len(columns):
                col += ' '
            else:
                col += ', '
            columns_str += col

        result = []

        args = None
        if not where:
            where_str = ''
        else:
            args = []
            where_str = ' WHERE '
            i = 0
            for key in where:
                if where[key] is None:
                    #Not add None arg to pass to request
                    pass
                elif isinstance(where[key], tuple) or isinstance(where[key], list):
                    args.append(where[key][1])
                else:
                    args.append(where[key])
                i += 1
                AND = ''
                if i < len(where):
                    AND = ' AND '

                if where[key] is None:
                    where_str += ('"' + key + '"' + " IS NULL " + AND)
                elif isinstance(where[key], tuple):
                    where_str += ('"' + key + '"' + " {sign} (%s) ".format(sign=where[key][0]) + AND)
                else:
                    where_str += ('"' + key + '"' + " = (%s) " + AND)

        order_by_str = ' ORDER BY {}'.format(id_name)
        if not order_by_id:
            order_by_str = ''

        command = "SELECT {columns_str} FROM {table_name} {where_str} {order_by_str};".format(
            columns_str=columns_str, table_name=table_name, where_str=where_str, order_by_str=order_by_str)

        try:
            cur = self.connection.cursor()
            cur.execute(command, args)
            for res in cur.fetchall():
                r = {}
                i = 0
                for data in res:
                    r[columns[i]] = data
                    i += 1
                result.append(r)
            cur.close()
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.commit()
            cur.close()
            raise Exception(str(e))
            return []

        return result

    select_from_where = getDataFromTable


    def executeSql(self, command):
        """
        Executing SQL Commands
        :param commands: str
        :return: 
        """
        try:
            cur = self.connection.cursor()
            cur.execute(command)
            self.connection.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.commit()
            cur.close()
            raise Exception(str(e))
        return

    def get_first_id_by_table_name(self, table_name):
        return self.get_last_id_by_table_name(table_name, last=False)

    def get_last_id_by_table_name(self, table_name, last=True):
        id_name = '"' + self.get_columns_by_table_name(table_name)[0] + '"'
        try:
            cur = self.connection.cursor()
            cur.execute("SELECT {id_name} FROM {table_name} ORDER BY id {desc} limit 1".format(id_name=id_name, table_name=table_name, desc="DESC" if last else ""))
            res = cur.fetchone()
            self.connection.commit()
            cur.close()
            return res[0]
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.commit()
            cur.close()
            raise e


def readConfig(section='postgresql', filename='/opt/ankf/etc/db_connection.conf'):
    parser = ConfigParser()
    parser.read(filename)
    result = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            result[param[0]] = param[1]

    return result

DB_CONFIG = readConfig()

def get_db_connection():
    """
    Returns the connection to the PostgreSQL database    
    :return: 
    """
    params = DB_CONFIG
    if not params:
        raise Exception("Config not found aborting")
        return None

    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.close()
    return conn