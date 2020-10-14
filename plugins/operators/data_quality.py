from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_col_dict={}
                 test_and_result={}
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_col_dict = table_col_dict
        self.test_and_result = test_and_result

    def execute(self, context):
        self.log.info('Getting credentials')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting tests')
        # for each set of table and col
        for table, col in self.table_col_dict.items():

            # run every test query in dict
            for query, expected in self.test_and_result.items():

                a_list = redshift_hook.get_records(query.format(col, table))
                result = a_list[0][0]
                #self.log.info(f"Running test on {tabel} table and {col} column with query:")
                #self.log.info(query.format(col, table))
                #self.log.info(f"Result = {result}, expected {expected}")

                if eval(result expected):
                    self.log.info(f"""\n--------------[TEST PASSED] - Data quality on {table} table is good.""")
                else:
                    raise ValueError(f"""\n--------------[TEST FAILED] - Data quality check failed on {table} table column {col},
                                                           \nExpected {expected}, but got {result}""")





        
