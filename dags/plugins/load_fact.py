from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.hooks.postgres_hook import PostgresHook

from airflow.models.baseoperator import BaseOperator
#from airflow.models import BaseOperator

from airflow.contrib.hooks.aws_hook import AwsHook

# decorator is deprecated and thus not needed anymore
#from airflow.utils.decorators import apply_defaults



class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    fact_sql_stmt = """
        insert into {}
        {};    
    """ 


######################################### INIT METHOD #########################################
    
    def __init__(
                self,
                redshift_conn_id = "",
                table = "",
                sql_stmt = "",
                append_only = False,
                 *args, 
                 **kwargs):


        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.append_only = append_only


######################################### EXECUTE METHOD #########################################

    def execute(self, context):
        self.log.info('Starting with Loading Task - FactTable')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        formatted_sql = LoadFactOperator.fact_sql_stmt.format(
            self.table,
            self.sql_stmt
        )

        if self.append_only:
            self.log.info("Appending new data into table {}".format(self.table))
            redshift_hook.run(formatted_sql)
        else:
            self.log.info("Truncating table {} to insert new data".format(self.table))
            redshift_hook.run("delete from {}".format(self.table))
            redshift_hook.run(formatted_sql)
        
