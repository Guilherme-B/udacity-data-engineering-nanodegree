from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """THe DataQualityOperator asserts the existance of records in the specified table(s).

    Raises:
        ValueError: [redshift_conn_id not specified]
        ValueError: [redshift_conn_id not found in Airflow connections]
        ValueError: [tables empty or not specified]
        ValueError: [no records located in table]
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables= [],
                 *args, 
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self._redshift_conn_id = redshift_conn_id
        self._tables = tables

    def execute(self, context):
        
        if self._redshift_conn_id is None:
            raise ValueError("DataQualityOperator: Failed to assert data quality, missing RedShift connection ID")
            
        if self._tables is None or len(self._tables) == 0:
            raise ValueError("DataQualityOperator: Failed to assert data quality, no table has been specified")
            
        redshift = PostgresHook(self._redshift_conn_id)
            
        for table in self._tables:            
            if table is None or table == '' or not isinstance(table, str):
                self.log.info('DataQualityOperator: skipping invalid table ', table)
                continue
            
            self.log.info('DataQualityOperator: asserting quality for table ', table)
            
            result = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(result[0]) < 1 or len(result) < 1:
                raise ValueError(f"DataQualityOperator: table {table} has no records")
                
            num_records = result[0][0]
            
            if num_records < 1:
                raise ValueError(f"DataQualityOperator: table {table} has no records")
                
                
            self.log.info(f"DataQualityOperator: table {table} check passed with {num_records} records")
        
        