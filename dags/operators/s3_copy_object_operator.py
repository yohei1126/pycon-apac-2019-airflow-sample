# -*- coding: utf-8 -*-

import logging
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3CopyObjectOperator(BaseOperator):
    template_fields = ('source_object_key', 'dest_object_key')

    @apply_defaults
    def __init__(
            self,
            source_object_key,
            dest_object_key,
            source_bucket_name=None,
            dest_bucket_name=None,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super(S3CopyObjectOperator, self).__init__(*args, **kwargs)

        self.source_object_key = source_object_key
        self.dest_object_key = dest_object_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        logging.info('copying from bucket = %s, key = %s to bucket = %s, key = %s',
            self.source_bucket_name, self.source_object_key, self.dest_bucket_name, self.dest_object_key)
        s3_hook.copy_object(self.source_object_key, self.dest_object_key,
                            self.source_bucket_name, self.dest_bucket_name)
