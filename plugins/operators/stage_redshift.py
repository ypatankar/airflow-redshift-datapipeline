from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
This custom operator stages data in Redshift from S3
"""

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        copy {}
        from '{}'
        access_key_id '{}'
        secret_access_key '{}'
        region 'us-west-2' compupdate off
        json '{}' truncatecolumns;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 *args, **kwargs):
        """
        This is the constructor for Stage S3 to Redshift operator that initializes the necessary parameters
        :param redshift_conn_id: Redshift connection details
        :param aws_credentials_id: aws credentials to copy data from S3
        :param table: table to be populated
        :param s3_bucket: S3 bucket name where files are stored
        :param s3_key: S3 key where files are stored
        :param json_path: S3 path to jsonpaths file
        :param args: context arguments
        :param kwargs: keyword arguments
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        """
        This method executes the stage S3 to Redshift task using the initialized parameters
        :param context: context object
        :return: None
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        if self.json_path == "":
            j_path = "auto"
        else:
            j_path = "s3://{}/{}".format(self.s3_bucket, self.json_path)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            j_path
        )
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)





