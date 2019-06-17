# aws
study of aws services

sqs -contains code for sqs queue producer and consumer using python and boto3

s3 - contains code for uploading and downloading files to s3 using python and boto3

spark/SparkStream - contains spark stream application, real time twitter stream is used to count tweets containing tag 'india' (using                         python and spark streaming)

Emr_s3_Dyanamo(hive) - contains POC code to run hive job on EMR (hive job runs on huge data set in S3 to gather aggregation result and writes                    result in s3), the result in s3 is stored in DynamoDb for further analysis.

Kinesis - contains code to create kinesis stream , send data to stream and consume data from stream using python and boto3.
