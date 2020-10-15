from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs tests to check data quality
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_col_dict={},
                 test_and_result={},
                 condition_dict={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_col_dict = table_col_dict
        self.test_and_result = test_and_result
        self.condition_dict = condition_dict

    def execute(self, context):
        self.log.info('Getting credentials')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting tests')
        # for each set of table and col
        for table, col in self.table_col_dict.items():

            # run every test query in dict
            for index, (query, expected) in enumerate(self.test_and_result.items()):

                a_list = redshift_hook.get_records(query.format(col, table))
                result = a_list[0][0]

                if self.condition_dict[index] == '==':
                    if result == expected:
                        self.log.info(f"""--------------[TEST PASSED] - Data quality on {table} table is good.""")
                    else:
                        raise ValueError(f"""--------------[TEST FAILED] - Data quality check failed on {table} table column {col},
                                                           \nExpected {expected}, but got {result}""")
                elif self.condition_dict[index] == '>':
                    if result > expected:
                        self.log.info(f"""--------------[TEST PASSED] - Data quality on {table} table is good.""")
                    else:
                        raise ValueError(f"""--------------[TEST FAILED] - Data quality check failed on {table} table column {col},
                                                           \nExpected {expected}, but got {result}""")
