from datetime import datetime, timedelta
from airflow.operators.redshift_operator import S3ToRedshiftOperator
from typing import Dict, List

from helpers import sql_queries_staging, sql_queries_presentation

# airflow
from airflow import DAG

# access airflow's system-wide config (variables)
#from airflow.configuration import conf
from airflow.models import Variable

# use BaseHook to access Connection credentials
from airflow.hooks.base_hook import BaseHook

# airflow operators
from airflow.operators.bash_operator import BashOperator

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


default_args = {
    'owner': 'guilhermebanhudo',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['admin@website.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('manifold_weekly_pipeline', default_args=default_args,
          schedule_interval=timedelta(weeks=1)
          )


s3_path: str = Variable.get("s3_path")
s3_path_template: str = Variable.get("s3_path_template")

aws_connection = BaseHook.get_connection("aws_credentials")
aws_username: str = aws_connection.login
aws_password: str = aws_connection.password

'''

    SCRAPERS
    
'''

scrapers_dummy = DummyOperator(task_id='scrapers_dummy',
                               dag=dag
                               )


'''

    STAGING LAYER PRE-PROCESSING
    (Spark consume Sources save to Parquet)
    
'''

SPARK_STEPS = [
    {
        'Name': 'Extract and Load source to Parquet',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode',
                    'cluster',
                    '--master',
                    'yarn',
                    's3a://' + s3_path + '/scripts/el_to_parquet.py',
                    '--execution_date={{ds}}',
                    '--aws_key=' + aws_username,
                    '--aws_secret=' + aws_password,
                    '--s3_bucket=' + s3_path,
                    '--s3_path_template=' + s3_path_template,
                ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Manifold ETL EMR',
    'LogUri': 's3://base-data-spark-listings/logs/',
    'ReleaseLabel': 'emr-6.2.0',
    # --jars /usr/lib/hadoop/hadoop-aws.jar
    'Applications': [
        {
            'Name': 'Spark'
        },
        {
            'Name': 'Hadoop'
        },
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.xlarge',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'BootstrapActions': [
        {
            'Name': 'string',
            'ScriptBootstrapAction': {
                'Path': 's3://{{ var.value.s3_path }}/scripts/bootstrap_install_python_modules.sh',
            }
        },
    ],
}


create_emr: EmrCreateJobFlowOperator = EmrCreateJobFlowOperator(
    task_id='start_emr',
    dag=dag,
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_credentials',
)

step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='start_emr', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('start_emr', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_credentials',
    )


'''

    STAGING TABLE CREATION
    
'''

# create the broker staging table
broker_staging_create = PostgresOperator(
    task_id='broker_staging_create',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=sql_queries_staging.broker_staging_create
)

# create the asset staging table
asset_staging_create = PostgresOperator(
    task_id='asset_staging_create',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=sql_queries_staging.asset_staging_create
)

# create the geography staging table
geography_staging_create = PostgresOperator(
    task_id='geography_staging_create',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=sql_queries_staging.geography_staging_create
)

# create the stock staging table
stock_staging_create = PostgresOperator(
    task_id='stock_staging_create',
    dag=dag,
    postgres_conn_id='redshift_conn',
    sql=sql_queries_staging.stock_staging_create
)

staging_table_create_dummy = DummyOperator(task_id='staging_table_create_dummy',
                                           dag=dag
                                           )

staging_table_creation: List[PostgresOperator] = [broker_staging_create, asset_staging_create,
                                                  geography_staging_create, stock_staging_create]


'''

    STAGING TABLE POPULATION
    (AWS Redshift COPY)
    
'''

staging_tasks: List[S3ToRedshiftOperator] = []

redshift_role_name = Variable.get("redshift_role_name")
redshift_region_name = Variable.get("redshift_region_name")

# populate the staging layer via Redshift's COPY
for object_name, config in sql_queries_staging.copy_query_definition:
    destination_name: str = config.destination_name
    source_name: str = config.source_name

    operator: S3ToRedshiftOperator = S3ToRedshiftOperator(
        task_id=object_name,
        dag=dag,
        redshift_conn_id='redshift_conn',
        redshift_credentials_id='redshift_credentials',
        s3_path=s3_path,
        s3_bucket_template=s3_bucket_template,
        destination_name=destination_name,
        source_name=source_name,
        role_name=redshift_role_name,
        region_name=redshift_region_name,
        provide_context=True
    )

    staging_tasks.append(operator)

staging_table_populate_dummy = DummyOperator(task_id='staging_table_populate_dummy',
                                             dag=dag
                                             )

'''

    PRESENTATION LAYER POPULATION
    (Redshift - Staging to Presentation)
    
'''

dimension_definitions: Dict[str, Dict[str, str]
                            ] = sql_queries_presentation.dimension_definitions

fact_definitions: Dict[str, str] = sql_queries_presentation.fact_definitions

presentation_dim_tasks: List[PostgresOperator] = []
presentation_fact_tasks: List[PostgresOperator] = []

# add the presentation layer's dimension tasks
for object_name, config in dimension_definitions:
    target_table: str = config.get('target_table')
    base_table: str = config.get('base_table')
    match_columns: List[str] = config.get('match_columns')

    query: str = sql_queries_presentation.generate_upsert_query(
        target_table=target_table, base_table=base_table, match_columns=match_columns)

    presentation_task: PostgresOperator = PostgresOperator(
        task_id=object_name,
        dag=dag,
        postgres_conn_id='redshift_conn',
        sql=query
    )

    presentation_dim_tasks.append(presentation_task)


presentation_dimension_dummy = DummyOperator(task_id='presentation_dimension_dummy',
                                             dag=dag
                                             )

# add the presentation layer's fact tasks
for object_name, query in fact_definitions:
    presentation_task: PostgresOperator = PostgresOperator(
        task_id=object_name,
        dag=dag,
        postgres_conn_id='redshift_conn',
        sql=query
    )

    presentation_fact_tasks.append(presentation_task)

'''

    DAG FLOW
    
'''


# 2) create the staging Parquet files should the scrapers complete
scrapers_dummy >> create_emr >> job_sensor

# 3) create the staging Redshift tables
staging_table_creation >> staging_table_create_dummy

# 4) populate the staging Redshift layer if the table creation was successful
staging_table_create_dummy >> staging_tasks >> staging_table_populate_dummy

# 5) populate the presentation Redshift layer's dimensions with SCD2
staging_table_populate_dummy >> presentation_dim_tasks >> presentation_dimension_dummy

# 6) populate the presentation Redshift layer's facts
presentation_dimension_dummy >> presentation_fact_tasks

'''
templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

scraper_century21 = BashOperator(
    task_id='scraper_century21',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)
'''
