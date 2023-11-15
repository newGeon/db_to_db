import os
import re
import time
import json

import mariadb
import datetime

import pandas as pd
import jaydebeapi

from tqdm import tqdm


if __name__=="__main__":

    # Tibero DB Connection
    print('==== Tibero DB Connection ===============================================================')
    conn_tibero = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        "jdbc:tibero:thin:@127.0.0.1:8629:tibero",
        ["geon_t", "1234"],
        "tibero6-jdbc.jar",
    )
    cur_tibero = conn_tibero.cursor()


    # MariaDB Local DB Connection 
    print('==== MariaDB Connection =================================================================')
    conn_mariadb = mariadb.connect(
        user="geon",
        password="1234",
        host="127.0.0.1",
        port=3306,
        database="nl2sql"
    )
    cur_mariadb = conn_mariadb.cursor()


    print('===== Tibero DATA SELECT ====================================================================')
    sql_basic = """ SELECT ID, COLLECT_SITE_ID, CATEGORY_BIG, CATEGORY_SMALL, DATA_NAME, DATA_DESCRIPTION, 
                           PROVIDE_DATA_TYPE, PROVIDE_URL_LINK, COLLECT_DATA_TYPE, COLLECT_URL_LINK, 
                           IS_COLLECT_YN, DATA_ORIGIN_KEY
                      FROM DATA_BASIC_INFO
                     WHERE ID IN (7824, 7806, 7862)
                """
    cur_tibero.execute(sql_basic)
    result_baisc = cur_tibero.fetchall()

    for one in result_baisc:
        print(one)
        data_id = one[0]

        # DATA Insert (MariaDB)
        mariadb_basic = """ INSERT INTO data_basic_info
                                   (id, collect_site_id, category_big, category_small, data_name, data_description, 
                                    provide_data_type, provide_url_link, data_origin_key, collect_data_type, 
                                    collect_url_link, is_collect_yn)
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """
        values_mariadb_basic = (one[0], one[1], one[2], one[3], one[4], one[5], one[6], one[7], one[11], one[8], one[9], one[10])
        cur_mariadb.execute(mariadb_basic, values_mariadb_basic)
        conn_mariadb.commit()

        sql_table = """ SELECT ID, DATA_BASIC_ID, LOGICAL_TABLE_KOREAN, LOGICAL_TABLE_ENGLISH, PHYSICAL_TABLE_NAME, 
                               PHYSICAL_CREATED_YN, DATA_INSERTED_YN, DATA_INSERT_ROW, ORIG_TABLE_NAME, 
                               TARGET_ROWS, JOIN_TABLE_ID, JOIN_COLUMN_ID, JOIN_TABLE_COLUMN_ID
                          FROM MANAGE_PHYSICAL_TABLE
                         WHERE DATA_BASIC_ID = ?        
                    """
        table_values = (data_id, )
        cur_tibero.execute(sql_table, table_values)
        result_table = cur_tibero.fetchall()

        one_table = result_table[0]
        
        table_id = one_table[0]
        table_logical = one_table[3]
        table_physical = one_table[4]

        # DATA Insert (MariaDB)
        mariadb_table = """ INSERT INTO manage_physical_table
                                   (id, data_basic_id, logical_table_korean, logical_table_english, physical_table_name, 
                                    physical_created_yn, data_inserted_yn, data_insert_row, orig_table_name, 
                                    target_rows, join_table_id, join_column_id, join_table_column_id)
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
        values_mariadb_table = (one_table[0], one_table[1], one_table[2], one_table[3], one_table[4], one_table[5], one_table[6], one_table[7], one_table[8], one_table[9], one_table[10], 
                                one_table[10], one_table[11], one_table[12])
        cur_mariadb.execute(mariadb_table, values_mariadb_table)
        conn_mariadb.commit()

        sql_column = """ SELECT ID, DATA_PHYSICAL_ID, LOGICAL_COLUMN_KOREAN, LOGICAL_COLUMN_ENGLISH, 
                                PHYSICAL_COLUMN_NAME, PHYSICAL_COLUMN_TYPE, IS_CREATED_YN, 
                                PHYSICAL_COLUMN_ORDER, IS_USE_YN
                           FROM MANAGE_PHYSICAL_COLUMN
                          WHERE DATA_PHYSICAL_ID = ?
                          ORDER BY PHYSICAL_COLUMN_ORDER
                     """
        column_values = (table_id, )
        cur_tibero.execute(sql_column, column_values)
        result_column = cur_tibero.fetchall()


        mariadb_create_sql = "CREATE TABLE " + table_logical + " (id int(11) NOT NULL AUTO_INCREMENT "

        for i_column in result_column:
            
            logical_column_name = i_column[3].lower()
            physical_column_name = i_column[4]
            physical_column_type = i_column[5]
            physical_create_type = 'VARCHAR(200)'

            # DATA Insert (MariaDB)
            mariadb_column = """ INSERT INTO manage_physical_column
                                       (id, data_physical_id, logical_column_korean, logical_column_english, 
                                        physical_column_name, physical_column_type, is_created_yn, 
                                        physical_column_order, is_use_yn)
                                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);

                            """
            values_mariadb_column = (i_column[0], i_column[1], i_column[2], i_column[3].lower(), i_column[4].lower(), i_column[5], i_column[6], i_column[7], i_column[8])
            cur_mariadb.execute(mariadb_column, values_mariadb_column)
            conn_mariadb.commit()

            """
            if physical_column_type == 'NUMBER':
                physical_create_type = 'int(11)'
            elif physical_column_type == 'VARCHAR':
                physical_create_type = 'VARCHAR(200)'
            else:
                physical_create_type = 'DATETIME'
            """

            mariadb_create_sql = mariadb_create_sql + ', ' + logical_column_name + ' ' + physical_create_type + ' DEFAULT NULL'

        # TABLE Create
        mariadb_create_sql = mariadb_create_sql + ' , PRIMARY KEY (id) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
        cur_mariadb.execute(mariadb_create_sql)
        conn_mariadb.commit()


        sql_data = " SELECT * FROM " + table_physical + " ORDER BY ID "
        cur_tibero.execute(sql_data)
        result_data = cur_tibero.fetchall()

        # DATA Insert
        for r_data in result_data:
            mariadb_insert_data = 'INSERT INTO ' + table_logical + ' VALUES ('
            value_isnert = ''

            for i in r_data:

                if str(type(i)) == "<java class 'JLong'>":
                    # 숫자
                    value_isnert += str(i) + ' ,'
                elif str(type(i)) == type(None):
                    # NULL
                    value_isnert += ' NULL' + ','
                elif str(type(i)) == "<java class 'JDouble'>":
                    # 더블
                    value_isnert += str(i) + ' ,'
                elif str(type(i)) == "<class 'NoneType'>":
                    value_isnert += str('NULL') + ' ,'
                else:
                    value_isnert += ' \'' + i[0:99] + '\',' 
            
            value_isnert = value_isnert[:-1]

            mariadb_insert_data = mariadb_insert_data + value_isnert + ' )'
            cur_mariadb.execute(mariadb_insert_data)
            conn_mariadb.commit()

    conn_mariadb.close()
    cur_tibero.close()
    print('=========================================================================================')    
