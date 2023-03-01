import pandas as pd
import pandahouse as ph
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230120',
                      'user':'student', 
                      'password':'XXXXXXXXXX'
                     }

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'XXXXXXXXXXXXX'
                     }

default_args = {
'owner': 'a.burlakov-9',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2023, 2, 3),
}

schedule_interval = '0 10 * * *'

metrics_order = ['event_date','dimension','dimension_value',\
                                 'views', 'likes', \
                                 'messages_received', 'messages_sent',\
                                 'users_received', 'users_sent']

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def burlakov_dag_less_6():    

    @task
    def extract_feed_activity():
        feed_query = """ 
            SELECT 
                user_id, gender, age, os,
                toDate(time) as event_date,
                sum(action='view') as views,
                sum(action='like') as likes
            FROM simulator_20230120.feed_actions 
            WHERE toDate(time) = yesterday()
            GROUP BY user_id, gender, age, os, toDate(time)
        """
        return ph.read_clickhouse(feed_query, connection=connection)

    @task
    def extract_messages_activity():
        messages_query = """
            SELECT *
            FROM (
                SELECT 
                    user_id, gender, age, os, toDate(time) as event_date,
                    count(reciever_id) as messages_sent,
                    count(distinct reciever_id) as users_sent
                FROM simulator_20230120.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id, gender, age, os, toDate(time)
            ) AS sent
            JOIN (
                SELECT 
                    reciever_id,
                    count(user_id) as messages_received,
                    count(distinct user_id) as users_received
                FROM simulator_20230120.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY reciever_id
            ) AS recieved
            on sent.user_id=recieved.reciever_id
        """
        return ph.read_clickhouse(messages_query, connection=connection)

    @task
    def concat_origin_tables(feed_table, mes_table):
        table = pd.merge(feed_table, mes_table, on=['user_id', 'gender', 'os', 'age', 'event_date'], how='outer').fillna(0)
        table[['views', 'likes', 'messages_sent', 'users_sent', 'reciever_id', 'messages_received',
       'users_received']] = table[['views', 'likes', 'messages_sent', 'users_sent', 'reciever_id', 'messages_received',
       'users_received']].astype('uint64')
        return table

    @task
    def gender_dimension(table):
        gender_table = table.groupby(['event_date','gender']) \
        ['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received'] \
        .sum().reset_index().rename(columns={'gender':'dimension_value'})
        gender_table['dimension'] = 'gender'
        gender_table['dimension_value'] = gender_table['dimension_value'].replace({0:'Female', 1:'Male'})
        return gender_table[metrics_order]

    @task
    def os_dimension(table):
        os_table = table.groupby(['event_date','os'])\
            ['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']\
            .sum().reset_index().rename(columns={'os':'dimension_value'})
        os_table['dimension'] = 'os'
        return os_table[metrics_order]

    @task
    def age_dimension(table):
        age_table = table.groupby(['event_date','age'])\
            ['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']\
            .sum().reset_index().rename(columns={'age':'dimension_value'})
        age_table['dimension'] = 'age'
        return age_table[metrics_order]

    @task
    def concat_final_tables(gender_table,os_table,age_table):
        return pd.concat([gender_table,os_table,age_table]).reset_index(drop=True)

    @task
    def upload_table(table):
        query = '''
        CREATE TABLE IF NOT EXISTS test.burlakov_less_6  
        (
        event_date Date,
        dimension String,
        dimension_value String,
        views Int64,
        likes Int64,
        messages_received Int64,
        messages_sent Int64,
        users_received Int64,
        users_sent Int64
        )
        ENGINE = MergeTree()
        ORDER BY event_date
        '''
        ph.execute(query, connection=connection_test)
        ph.to_clickhouse(table, 'burlakov_less_6', 
                         connection=connection_test, index=False)

    
    feed_table = extract_feed_activity()
    mes_table = extract_messages_activity()
    origin_table = concat_origin_tables(feed_table, mes_table)
    gender_table = gender_dimension(origin_table)
    os_table = os_dimension(origin_table)
    age_table = age_dimension(origin_table)
    final_table = concat_final_tables(gender_table,os_table,age_table)
    upload_table(final_table)
    
burlakov_dag_less_6 = burlakov_dag_less_6()


