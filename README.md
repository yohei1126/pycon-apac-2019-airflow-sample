# pycon-apac-2019-airflow-sample
Sample code for PyCon APAC 2019

## Install

* install docker
* create a file named `airflow.env`
* add AWS IAM user credential to access s3 buckets
  * Note: replade `your-access-key` and `your-private-key` with keys in your credential

```
AIRFLOW_CONN_AWS_DEFAULT=s3://your-access-key:your-private-key@S3:12345
```

## Run container

```
$ docker-compose up --build
```