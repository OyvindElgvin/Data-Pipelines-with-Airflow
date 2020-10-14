from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_stm="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_stm = sql_stm

    def execute(self, context):
        self.log.info('Getting credentials for {self.table} table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        truncate = f"""TRUNCATE public.{self.table}
                    """
        insert = f""" INSERT INTO {self.table}
                      {self.sql_stm}
                  """
        self.log.info("Dropping {self.table} table")
        redshift.run(truncate)
        redshift.run(insert)
