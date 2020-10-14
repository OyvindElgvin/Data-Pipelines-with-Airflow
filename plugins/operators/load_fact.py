from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'


    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_stm="",
                 append_data="",   # flytt denne til dimension tablesene
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_stm = sql_stm
        self.append_data = append_data

    def execute(self, context):
        self.log.info('Getting credentials for {self.table} table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_data:
            stm = f"""INSERT INTO public.{self.table}
                       ({self.sql_stm})
                   """
            self.log.info("Appending data into {self.table}")
            redshift.run(stm)
        else:
            drop = f""" DROP TABLE IF EXISTS public.{self.table}"""
            insert = f""" INSERT INTO {self.table}
                          {self.sql_stm}
                      """
            self.log.info("Dropping {self.table} table")
            redshift.run(drop)
            self.log.info("Inserting data into {self.table} table")
            redshift.run(insert)
