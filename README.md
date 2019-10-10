# pycon-apac-2019-airflow-sample
Sample code for PyCon APAC 2019

## Install

* install docker
* copy `airflow.env.tmp` to `airflow.env`. `airflow.env` is a configuration file for you environment.
* add AWS IAM user credential to access s3 buckets
  * Note: replade `your-access-key` and `your-private-key` with keys in your credential
* change S3 buckets name based on your environment

```
AIRFLOW_CONN_AWS_DEFAULT=s3://your-access-key:your-private-key@S3:12345
SOURCE_BUCKET=your-bucket-jp
DEST_BUCKET_SG=your-bucket-sg
DEST_BUCKET_EU=your-bucket-eu
DEST_BUCKET_US=your-bucket-us
```

## Run container

```
$ docker-compose up --build
```