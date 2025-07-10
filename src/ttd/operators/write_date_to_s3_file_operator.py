from airflow.models import BaseOperator
from botocore.exceptions import NoCredentialsError

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage


class WriteDateToS3FileOperator(BaseOperator):
    template_fields = (
        "date",
        "s3_key",
    )

    def __init__(self, s3_bucket, s3_key, date, append_file, *args, **kwargs):
        super(WriteDateToS3FileOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.date = date
        self.append_file = append_file

    def execute(self, context):
        try:
            aws_storage = AwsCloudStorage(conn_id="aws_default")

            # Modify the content (example: appending/override a line at the end)
            if aws_storage.check_for_key(key=self.s3_key, bucket_name=self.s3_bucket):
                old_content = aws_storage.read_key(key=self.s3_key, bucket_name=self.s3_bucket)
                if not old_content:
                    modified_content = self.date
                else:
                    content_array = old_content.split("\n")
                    content_array.sort(reverse=True)
                    if content_array[0] >= self.date:
                        self.log.info("Date is older than current file, not update")
                        return
                    if self.append_file:
                        content_array.insert(0, self.date)
                        # only keep first 30 version
                        modified_content = "\n".join(content_array[0:30])
                    else:
                        modified_content = self.date
            else:
                modified_content = self.date

            # Write the modified content back to the S3 file
            aws_storage.load_string(
                string_data=modified_content,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True,
            )

            self.log.info("Modified content saved to S3 file: " + self.s3_key)
        except NoCredentialsError:
            self.log.error("No AWS credentials found")
