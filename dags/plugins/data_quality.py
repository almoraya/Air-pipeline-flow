from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.hooks.postgres_hook import PostgresHook

from airflow.models.baseoperator import BaseOperator
#from airflow.models import BaseOperator

# decorator is deprecated and thus not needed anymore
#from airflow.utils.decorators import apply_defaultss

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'


######################################### INIT METHOD #########################################

    def __init__(
        self,
        redshift_conn_id = "",
        dq_checks = [],
        *args, 
        **kwargs):


        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks


######################################## EXECUTE METHOD #########################################

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        logging.info("Starting with data quality checks")

        for i, dq_check in enumerate(self.dq_checks):
            records = redshift_hook.get_records(dq_check['test_sql'])
            if not dq_check['expected_result'] == records[0][0]:
                   raise ValueError(f"Data quality check #{i} failed. {'The query: << '+ dq_check.get('test_sql', i) + ' >> fail to pass'}")