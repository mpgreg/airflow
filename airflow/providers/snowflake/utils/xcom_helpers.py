#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from urllib import parse as parser
import os
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils import _try_parse_snowflake_xcom_uri

def get_snowflake_xcom_objects() -> dict:
    """
    Checks necessary environment variables for necessary settings and returns a dict.
    """
    try: 
        snowflake_conn_id = os.environ['AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME']
    except:
        raise AirflowException("AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME environment variable not set.")
    
    try: 
        snowflake_xcom_table = os.environ['AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE']
    except: 
        raise AirflowException('AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE environment variable not set')
    
    assert len(snowflake_xcom_table.split('.')) == 3, "AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE is not a fully-qualified Snowflake table objet"
    
    try: 
        snowflake_xcom_stage = os.environ['AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE']
    except: 
        raise AirflowException('AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE environment variable not set')
    
    assert len(snowflake_xcom_stage.split('.')) == 3, "AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE is not a fully-qualified Snowflake stage objet"

    return {'conn_id': snowflake_conn_id, 'table': snowflake_xcom_table, 'stage': snowflake_xcom_stage}

def check_xcom_conn(snowflake_conn_id:str) -> bool:
    """Make sure we can connect to Snowflake before trying to push xcoms."""
    
    response = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).test_connection()
    if response[0] != True:
        raise AirflowException(f"Snowflake XCOM connection {snowflake_conn_id} error. {response[1]}")
    
    return True

def check_xcom_table(snowflake_conn_id:str, snowflake_xcom_table:str) -> bool:
    """
    Checks the schema of the XCOM table against expected schema and returns True or raises an exception.
    """
    expected_schema = [
        ('DAG_ID', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('TASK_ID', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('RUN_ID', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('MULTI_INDEX', 'NUMBER(38,0)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('KEY', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('VALUE_TYPE', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None), 
        ('VALUE', 'VARCHAR(16777216)', 'COLUMN', 'N', None, 'N', 'N', None, None, None, None)
    ]

    xcom_table_schema = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_records(f'DESCRIBE TABLE {snowflake_xcom_table}')

    if xcom_table_schema != expected_schema:
        raise AirflowException(f"""
                XCOM table {snowflake_xcom_table} does not have the correct schema. 
                Please create it with:

                SnowflakeHook().run(\'\'\'CREATE TABLE {snowflake_xcom_table} 
                    ( 
                        dag_id varchar NOT NULL, 
                        task_id varchar NOT NULL, 
                        run_id varchar NOT NULL,
                        multi_index integer NOT NULL,
                        key varchar NOT NULL,
                        value_type varchar NOT NULL,
                        value varchar NOT NULL
                    )\'\'\')
                """)
    return True

def check_xcom_stage(snowflake_conn_id:str, snowflake_xcom_stage:str) -> bool:
    """
    Check if XCOM stage is accessible and returns True or raises and exception.
    """
    
    try:
        SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_records(f'DESCRIBE STAGE {snowflake_xcom_stage}')
    except Exception as e:
        if 'does not exist or not authorized' in e.msg:
            raise AirflowException(
                f'''
                XCOM stage {snowflake_xcom_stage} does not exist or not authorized. 
                Please create it with:

                SnowflakeHook().run('CREATE STAGE {snowflake_xcom_stage}')
                '''
            )
        else:
            raise e
        
    return True

def check_xcom_backend() -> bool:
    """
    Checks all XCOM backend components before attempting to push XCOMs and returns
    True or raises an exception.
    """

    snowflake_xcom_objects = get_snowflake_xcom_objects()
    
    assert check_xcom_conn(snowflake_conn_id=snowflake_xcom_objects['conn_id']) 

    assert check_xcom_table(
        snowflake_conn_id=snowflake_xcom_objects['conn_id'], 
        snowflake_xcom_table=snowflake_xcom_objects['table']
        )

    assert check_xcom_stage(
        snowflake_conn_id=snowflake_xcom_objects['conn_id'], 
        snowflake_xcom_stage=snowflake_xcom_objects['stage']
        )

    return True
