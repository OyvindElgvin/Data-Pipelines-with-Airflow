from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator plugin that append or truncate - insert into target dimension table

    Parameters:
        redshift_conn_id   credentials to redshift
        table              target table
        sql_stm            sql statement for append or truncate - insert
        append_data        append data with True or False
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_stm="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stm = sql_stm
        self.append_data = append_data

    def execute(self, context):
        self.log.info('Getting credentials for {self.table} table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        # appends if append_data == True, truncate if append_data == False
        if self.append_data:
            stm = f"""INSERT INTO {self.table}
                       ({self.sql_stm})
                   """
            self.log.info("Appending data into {self.table}")
            redshift.run(stm)

        else:
            self.log.info("Truncating {self.table} table")
            truncate = f""" TRUNCATE {self.table}"""
            redshift.run(truncate)

            self.log.info("Inserting data into {self.table} table")
            insert = f""" INSERT INTO {self.table}
                          {self.sql_stm}
                      """
            redshift.run(insert)
