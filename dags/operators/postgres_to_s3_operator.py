import json
import sys
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from tempfile import NamedTemporaryFile

PY3 = sys.version_info[0] == 3

class PostgresToS3Operator(BaseOperator):
    """
    Copy data from Postgres to S3 in CSV format.
    """
    template_fields = ('sql', 'bucket', 'object_key')

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 object_key,
                 postgres_conn_id='postgres_default',
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        """
        :param sql: The SQL to execute on the Postgres table.
        :type sql: str
        :param bucket: The bucket to upload to.
        :type bucket: str
        :param object_key: The object_key to use as the object name when uploading to S3.
        :type object_key: str
        :param postgres_conn_id: Reference to a specific Postgres hook.
        :type postgres_conn_id: str
        :param aws_conn_id: Reference to a specific AWS connection hook.
        :type aws_conn_id: str
        """
        super(PostgresToS3Operator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.object_key = object_key
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        with self._query_postgres() as cursor:
            files_to_upload = self._write_local_data_files(cursor)

            # Flush all files before uploading
            for file_handle in files_to_upload.values():
                file_handle.flush()

            self._upload_to_s3(files_to_upload)

            # Close all temp file handles.
            for file_handle in files_to_upload.values():
                file_handle.close()

    def _query_postgres(self):
        """
        Queries Postgres and returns a cursor to the results.
        """
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()
        logging.info('executing sql: {}'.format(self.sql))
        cursor.execute(self.sql)
        return cursor

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.
        :return: A dictionary where keys are object_keys to be used as object
            keys in S3, and values are file handles to local files that
            contain the data for the S3 objects.
        """
        schema = list(map(lambda schema_tuple: schema_tuple[0], cursor.description))
        logging.info('schema = %s', schema)
        tmp_file_handles = {}
        row_no = 0

        def _create_new_file():
            handle = NamedTemporaryFile(delete=True)
            object_key = self.object_key.format(len(tmp_file_handles))
            tmp_file_handles[object_key] = handle
            return handle

        # Don't create a file if there is nothing to write
        if cursor.rowcount > 0:
            tmp_file_handle = _create_new_file()

            for row in cursor:
                # Convert datetime objects to utc seconds, and decimals to floats
                #row = map(self.convert_types, row)
                row_dict = dict(zip(schema, row))

                s = json.dumps(row_dict, sort_keys=True)
                if PY3:
                    s = s.encode('utf-8')
                tmp_file_handle.write(s)

                # Append newline to make dumps BigQuery compatible.
                tmp_file_handle.write(b'\n')

                # Stop if the file exceeds the file size limit.
                #if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                #    tmp_file_handle = _create_new_file()
                row_no += 1

        self.log.info('Received %s rows over %s files', row_no, len(tmp_file_handles))

        return tmp_file_handles

    def _upload_to_s3(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to S3.
        """
        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        for object, tmp_file_handle in files_to_upload.items():
            logging.info('_upload_to_s3: file = %s, bucket = %s, key = %s', tmp_file_handle.name, self.bucket, object)
            hook.load_file_obj(file_obj=tmp_file_handle, key=object, bucket_name=self.bucket, replace=True)
