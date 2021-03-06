from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Responsible for executing a query against the staging area to generate a Dimension.

    Raises:
        ValueError: [table has not been specified]
        ValueError: [statement has not been specified]
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_stmt,
                 append= False,
                 *args, 
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self._redshift_conn_id = redshift_conn_id
        self._table = table
        self._sql_stmt = sql_stmt
        self._append = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id= self._redshift_conn_id)
        
        self.log.info(f"LoadDimensionOperator: loading dimension {self._table}")
        
        if self._table is None or self._table == '':
            raise ValueError("LoadDimensionOperator: loading dimension failed: no table has been specified.")
           
        if self._sql_stmt is None or self._sql_stmt == '':
            raise ValueError("LoadDimensionOperator: loading dimension failed: no statement has been specified.")
            
        full_statement = ""
        
        if self._append:
            full_statement = """
                    BEGIN;
                    INSERT INTO {table}
                    {statement};
                    COMMIT;""".format(table=self._table, statement=self._sql_stmt)
        else:
            full_statement = """
                    BEGIN;
                    TRUNCATE TABLE {table}; 
                    INSERT INTO {table}
                    {statement}; 
                    COMMIT;""".format(table=self._table, statement=self._sql_stmt)
            
        redshift.run(full_statement)
