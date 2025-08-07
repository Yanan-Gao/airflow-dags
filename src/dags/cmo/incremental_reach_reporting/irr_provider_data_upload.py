"""
Daily DAG to copy TvSquared campaign details into S3.
"""

import logging

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage

sql_connection = 'provisioning_replica'  # provdb-bi.adsrvr.org
db_name = 'Provisioning'


class IrrProviderDataUpload:

    def __init__(self, s3_bucket, s3_base_path, provisioning_query, use_acl=False):
        self.s3_bucket = s3_bucket
        self.s3_base_path = s3_base_path
        self.provisioning_query = provisioning_query
        self.use_acl = use_acl

    def load_and_write_provider_data_to_s3(self, **context):
        sql_hook = MsSqlHook(mssql_conn_id=sql_connection, schema=db_name)
        campaignDetails = sql_hook.get_pandas_df(self.provisioning_query)
        outputDf_str = campaignDetails.to_csv()

        logging.info(outputDf_str)

        aws_storage = AwsCloudStorage(conn_id='aws_default')
        aws_storage.load_string(
            string_data=outputDf_str,
            key=self.s3_base_path + context['data_interval_start'].strftime("%Y-%m-%d") + '.csv',
            bucket_name=self.s3_bucket,
            replace=True,
            acl_policy='bucket-owner-full-control' if self.use_acl else None
        )
