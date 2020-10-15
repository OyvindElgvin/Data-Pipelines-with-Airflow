from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator plugin that append/INSERTs into target fact table

    Parameters:
        redshift_conn_id   credentials to redshift
        table              target table
        sql_stm            sql statement for insert
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_stm="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stm = sql_stm

    def execute(self, context):
        self.log.info('Getting credentials for {self.table} table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        append = f"""INSERT INTO public.{self.table}
                       ({self.sql_stm})
                   """
        self.log.info("Appending data into {self.table} table")
        redshift.run(append)






        
