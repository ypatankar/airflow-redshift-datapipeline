from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
This custom operator loads data in fact tables
"""

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    facts_sql_template = """
    INSERT INTO {} {}
    """
    
    facts_truncate_template = """
    TRUNCATE {}
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_query="",
                 mode="",
                 *args, **kwargs):
        """
        This is the constructor for Load Fact Operator that initializes the necessary parameters
        :param redshift_conn_id: redshift connection details
        :param table_list: table name
        :param sql_query: the sql query to be executed by the Operator
        :param mode: execution mode of the sql query
        :param args: context arguments
        :param kwargs: keyword arguments
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        """
        This method executes the load fact task using the initialized parameters
        :param context: context object
        :return: None
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == "append":
            formatted_sql = LoadFactOperator.facts_sql_template.format(self.table, self.sql_query)
            redshift.run(formatted_sql)
        elif self.mode == "truncate-insert":
            delete_sql = LoadFactOperator.facts_truncate_template.format(self.table)
            redshift.run(delete_sql)
            formatted_sql = LoadFactOperator.facts_sql_template.format(self.table, self.sql_query)
            redshift.run(formatted_sql)

