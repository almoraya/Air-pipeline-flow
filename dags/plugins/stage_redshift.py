from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.hooks.postgres_hook import PostgresHook

from airflow.models.baseoperator import BaseOperator
#from airflow.models import BaseOperator

from airflow.contrib.hooks.aws_hook import AwsHook

# decorator is deprecated and thus not needed anymore
#from airflow.utils.decorators import apply_defaults



######################################### CLASS #########################################

# Log data: s3://udacity-dend/log_data
# Song data: s3://udacity-dend/song_data

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key")

    copy_sql = """
        copy {}
        from '{}'
        access_key_id '{}'
        secret_access_key '{}'
        region '{}'    
        json {}
    """


######################################### INIT METHOD #########################################

    def __init__(
                self,
                redshift_conn_id = "",
                aws_credentials_id = "",
                table = "",
                s3_bucket = "",
                s3_key = "",
                region = "",
                extra_params="",
                *args,
                **kwargs):


        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.extra_params = extra_params
        


######################################### EXECUTE METHOD #########################################

    def execute(self, context):
        self.log.info('Starting with Staging Task')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift_hook.run("delete from {}".format(self.table))

        self.log.info("Copying data from s3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        # s3://udacity-dend/log_json_path.json   # s3://udacity-dend/song_data
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.extra_params
        )

        redshift_hook.run(formatted_sql)






