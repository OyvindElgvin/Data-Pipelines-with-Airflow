from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table_list="",
                 columns="",
                 sql_stm="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table_list = table_list
        self.columns = columns
        self.redshift_conn_id = redshift_conn_id
        self.sql_stm = sql_stm

    def execute(self, context):
        self.log.info('Starting DataQualityOperator')
        #aws_hook = AwsHook(self.aws_credentials_id)
        #credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        """
        For example one test could be a SQL statement that checks if certain column
        contains NULL values by counting all the rows that have NULL in the column.
        We do not want to have any NULLs so expected result would be 0 and the test
        would compare the SQL statement's outcome to the expected result.
        """
        for table, col in zip(self.table_list, self.columns):

            # check if sql statement is correct
            # self.log.info(self.sql_stm.format(col, col, table))

            a_list = redshift_hook.get_records(self.sql_stm.format(col, col, table))

            a_tuple = a_list[0]
            an_int_fianlly_ffs = a_tuple[0]

            # null test
            if an_int_fianlly_ffs > 0:
                raise ValueError(f"""\n----[TEST FAILED] - Data quality check failed.
                                                       There are {an_int_fianlly_ffs} in column {col} in table {table}""")
            else:
                self.log.info(f"\n----[TEST PASSED] - Data quality on table {table} check passed. {table} had {an_int_fianlly_ffs} nulls")


            # actual records test 1
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"\n----[TEST FAILED] - Data quality check failed. {table} returned no results")

            # actual records test 2
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"\n----[TEST FAILED] - Data quality check failed. {table} contained 0 rows")
            self.log.info(f"\n----[TEST PASSED] - Data quality on table {table} check passed with {records[0][0]} records")








        
