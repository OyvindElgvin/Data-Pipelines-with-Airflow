from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs tests to check data quality

    parameters:
        redshift_conn_id          credentials to redshift
        table_col_dict            dict with tables and columns
        test_and_result           dict with test querys and results
        comparison_operation_list list with for which comparison operator to use

    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_col_dict={},
                 test_and_result={},
                 comparison_operation_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_col_dict = table_col_dict
        self.test_and_result = test_and_result
        self.comparison_operation_list = comparison_operation_list

    def execute(self, context):
        self.log.info('Getting credentials')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting tests')
        # for each set of table and selected column
        for table, col in self.table_col_dict.items():

            # run every test query in dict
            for index, (query, expected) in enumerate(self.test_and_result.items()):

                a_list = redshift_hook.get_records(query.format(col, table, col))
                result = a_list[0][0]

                # if you need other conditions than these, just add them on
                if self.comparison_operation_list[index] == '==':
                    if result == expected:
                        self.log.info(f"""--------------[TEST PASSED] - Data quality on {table} table is good.""")
                    else:
                        raise ValueError(f"""--------------[TEST FAILED] - Data quality check failed on {table} table column {col},
                                                           \nExpected {expected}, but got {result}""")


                elif self.comparison_operation_list[index] == '>':
                    if result > expected:
                        self.log.info(f"""--------------[TEST PASSED] - Data quality on {table} table is good.""")
                    else:
                        raise ValueError(f"""--------------[TEST FAILED] - Data quality check failed on {table} table column {col},
                                                           \nExpected {expected}, but got {result}""")


                elif self.comparison_operation_list[index] == '<':
                    if result < expected:
                        self.log.info(f"""--------------[TEST PASSED] - Data quality on {table} table is good.""")
                    else:
                        raise ValueError(f"""--------------[TEST FAILED] - Data quality check failed on {table} table column {col},
                                                           \nExpected {expected}, but got {result}""")


                else:
                    raise ValueError(f"""--------------[TEST NOT INITIATED] SET A PROPER COMPARISON OPERATION TO INITIATE TEST.
                                                       \nThis is the test: {query.format(col, table, col)}""")
