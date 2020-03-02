from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

"""
This custom operator runs quality checks on a list of tables
"""

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list=[],
                 *args, **kwargs):
        """
        This is the constructor for Data Quality Operator that initializes the necessary parameters
        :param redshift_conn_id: redshift connection details
        :param table_list: list of tables to be quality checked
        :param args: context arguments
        :param kwargs: keyword arguments
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_list = table_list
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        This method executes the Data Quality task using the initialized parameters
        :param context: context object
        :return: None
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.table_list:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")