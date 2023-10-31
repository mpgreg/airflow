 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

All Snowpark decorators use the ``snowflake_conn_id`` argument to connect to 
Snowflake where the connection metadata is structured as follows:

.. list-table:: Snowflake Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Login: string
     - Snowflake user name
   * - Password: string
     - Password for Snowflake user
   * - Schema: string
     - Set schema to execute SQL operations on by default
   * - Extra: dictionary
     - ``warehouse``, ``account``, ``database``, ``region``, ``role``, ``authenticator``

  Alternatively set the connection with JSON environment variables 
  such as:
  .. example:: 
    :language: bash

    export AIRFLOW_CONN_SNOWFLAKE_DEFAULT='{"conn_type": "snowflake", "login": "<USER_NAME>", "password": "<PASSWORD>", "schema": "${DEMO_SCHEMA}", "extra": {"account": "<ORG_NAME>-<ACCOUNT_NAME>", "warehouse": "<WAREHOUSE_NAME>", "database": "${DEMO_DATABASE}", "region": "<REGION_NAME>", "role": "<USER_ROLE>", "authenticator": "snowflake", "session_parameters": null, "application": "AIRFLOW"}}'

  .. note::

  Snowflake `account identifier policies 
  <https://docs.snowflake.com/en/user-guide/admin-account-identifier>`__ recommends 
  using the ORG_NAME-ACCOUNT_NAME format.  Snowpark Containers are not supported with 
  the legacy account identifiers (ie. xxx111111.prod1.us-west-2.aws )

.. _howto/decorator:snowpark_python_task:

snowpark_python_task
=================

Python callable wrapped within the ``@task.snowpark_python_task`` decorator to run 
Snowpark Python dataframe API commands in 
`Snowflake <https://docs.snowflake.com/en/developer-guide/snowpark/python/index>`__.

Using the Decorator
^^^^^^^^^^^^^^^^^^
This decorator assumes that Snowpark libraries are installed on the Apache Airflow instance and, 
by definition, that the Airflow instance is running a version of python which is supported with 
Snowpark.  If not consider using a virtual environment with the snowpark_virtualenv_task or 
snowpark_ext_python_task decorator instead.

Parameters
----------

The decorator inherits from `virtualenv_task <../../apache-airflow/howto/operator/python.rst>`
and accepts all parameters in addition to the following:

python_callable
  The python unction to decorate
multiple_outputs
  If set to True, the decorated function's return value will be unrolled to
  multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. 
  Defaults to False.
snowflake_conn_id or conn_id
  A Snowflake connection name.  Default 'snowflake_default'
warehouse
 The name of a Snowflake warehouse (will override any warehouse defined in the connection)
database
  The name of a Snowflake database (will override the database defined in the connection)
schema
  The name of a Snowflake schema (will override the database defined in the connection)
role
  The name of a Snowflake role (will override the database defined in the connection)
authenticator
  The authenticator for Snowflake (will override the database defined in the connection)
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
session_parameters
  Set session-level parameters at the time you connect to Snowflake
log_level
  Set log level for Snowflake logging.  Default: 'ERROR'
temp_data_output
  If set to 'stage' or 'table' Snowpark DataFrame objects returned from the decorator 
  will be serialized to the stage specified by 'temp_data_stage' or a table with 
  prefix 'temp_data_table_prefix'.
temp_data_db
  The database to be used in serializing temporary Snowpark DataFrames. If not set the 
  decorator will use the database set at the decorator or hook level.  If None, the decorator 
  will assume a default database is set in the Snowflake user preferences.
temp_data_schema
  The schema to be used in serializing temporary Snowpark DataFrames. If not set the 
  decorator will use the schema set at the decorator or hook level.  If None, the decorator 
  will assume a default schema is set in the Snowflake user preferences.
temp_data_stage
  The stage to be used in serializing temporary Snowpark DataFrames. This must be set if 
  temp_data_output == 'stage'.  Output location will be named for the task:
    <DATABASE>.<SCHEMA>.<STAGE>/<DAG_ID>/<TASK_ID>/<RUN_ID>
        
      and a uri will be returned to Airflow xcom:
        
    snowflake://<ACCOUNT>.<REGION>?&stage=<FQ_STAGE>&key=<DAG_ID>/<TASK_ID>/<RUN_ID>/0/return_value.parquet'
temp_data_table_prefix
  The prefix name to use for serialized Snowpark DataFrames. This must be set if 
  temp_data_output == 'table'. Default prefix is "XCOM_".

    Output table will be named for the task:
    <DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX

      and the return value set to a SnowparkTable object with the fully-qualified table name.
    
    SnowparkTable(name=<DATABASE>.<SCHEMA>.<PREFIX><DAG_ID>__<TASK_ID>__<TS_NODASH>_INDEX)
temp_data_overwrite
  True or False: Whether to overwrite existing temp data or error.


An example usage of the SnowflakeOperator is as follows:

.. example::
    :language: python
    :start-after: [START howto_operator_snowflake]
    :end-before: [END howto_operator_snowflake]
    :dedent: 4


.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).

