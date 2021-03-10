from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Takes an S3 bucket's path's content and inserts the data into a Redshift table using COPY.


    Raises:
        ValueError: [failed to construct S3 key path]
        ValueError: [failed to construct S3 path]
    """    
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id = "",
                 redshift_conn_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 json_path = "",
                 delimiter = ",",
                 ignore_headers =  1,                 
                 *args, 
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self._aws_credentials_id = aws_credentials_id
        self._redshift_conn_id = redshift_conn_id
        self._table = table
        self._s3_bucket = s3_bucket
        self._s3_key = s3_key
        self._json_path = json_path
        self._delimiter = delimiter
        self._ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self._aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self._redshift_conn_id)
        
        rendered_key = self._s3_key.format(**context)
        s3_path = f"s3://{self._s3_bucket}/{rendered_key}"
        
        self.log.info(f"StageToRedshiftOperator: copying data from S3 {self._s3_key} to table {self._table}")
        
        if rendered_key is None or rendered_key == '':
            raise ValueError("StageToRedshiftOperator: S3 key path is missing")
            
        if s3_path is None or s3_path == '':
            raise ValueError("StageToRedshiftOperator: S3 path is missing")
        
        cmd = f"COPY {self._table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY" \
                f" '{credentials.secret_key}' JSON '{self._json_path}' COMPUPDATE OFF"
        
        redshift.run(cmd)