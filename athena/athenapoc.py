#-------------------------------------------------------------------------------
# Name:        athenapoc.py
# Purpose:
#
# Author:      abhishek.karkamkar
#
# Created:     08/04/2019
# Copyright:   (c) abhishek.karkamkar 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import boto3
import time
import json
import config
import requests
from requests_aws4auth import AWS4Auth

#Enter your ACCESS_KEY and ACCESS_ID
aws_region = config.aws_region
access_id = config.access_id
access_key = config.access_key

def GetFileData(s3Client, Bucket, Key):
    try:
        response = s3Client.get_object(
                Bucket=Bucket,
                Key=Key
                )
    except Exception as e:
        print("s3Client.get_object raised exception => "+str(e))
        return 1,''

    try:
        res = response['Body'].read().decode('utf-8')#.split()
    except Exception as e:
        print("response['Body'].read().decode('utf-8') raised exception => "+str(e))
        return 1,''

    return 0,res

def DeleteS3Files(s3Client, Bucket, Key):
    items = [Key,Key+'.metadata']
    for filekey in items:
        try:
            s3Client.delete_object(
                Bucket=Bucket,
                Key=filekey
            )
        except Exception as e:
            print("s3Client.delete_object() raised exception => " + str(e))
            return 1

    return 0


def FetchResponse(athenaClient, queryExecId):
    while True:
        try:
            response = athenaClient.get_query_execution(
                QueryExecutionId=queryExecId
            )
        except Exception as e:
            print("athenaClient.get_query_execution() raised exception => "+str(e))
            return 1,''


        filename = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        state = response['QueryExecution']['Status']['State']

        if state == 'QUEUED' or state == 'RUNNING':
                time.sleep(1)
                continue
        elif state == 'SUCCEEDED':
                #print('Query SUCCEEDED')
                break
        elif state == 'FAILED' or state == 'CANCELLED':
                print('Query Failed/canceled')
                return 1,''

    ret,s3Client = GetS3ConnObj()
    if 1 == ret:
        print('GetS3ConnObj() Failed')
        return 1,''

    Key = filename[filename.find(config.Key+'/'):]

    ret,response = GetFileData(s3Client, config.Bucket, Key)
    if 1 == ret:
        print('GetFileData() Failed')
        return 1,''

    ret = DeleteS3Files(s3Client, config.Bucket, Key)
    if 1 == ret:
        print('DeleteS3Files() Failed')
        return 1,''

    res = []
    response = response.split('\n')
    response.remove('')
    for ele in response:
        ele = ele.replace('"', '')
        res.append(ele)
    return 0,res

def RunQuery(athenaClient, database, query):
    try:
        response = athenaClient.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': config.s3file
            }
        )
    except Exception as e:
        print("athenaClient.start_query_execution() raised exception => "+str(e))
        return 1,''

    queryExecId = response['QueryExecutionId']

    ret,response = FetchResponse(athenaClient, queryExecId)
    if 1 == ret:
        print('FetchResponse() Failed')
        return 1,''

    return 0,response


def GetAthenaConnObj():
    session = boto3.Session(
    aws_access_key_id=access_id,
    aws_secret_access_key=access_key,
    region_name=aws_region
    )

    # Create Athena client
    try:
        athenaClient = session.client('athena')
    except Exception as e:
        print("session.client('athena') raised exception => "+str(e))
        return 1,None

    return 0,athenaClient

def GetS3ConnObj():

    session = boto3.Session(
    aws_access_key_id=access_id,
    aws_secret_access_key=access_key,
    region_name=aws_region
    )

    # Create s3 client
    try:
        s3Client = session.client('s3')
    except Exception as e:
        print("session.client('s3') raised exception => "+str(e))
        return 1,None

    return 0,s3Client


def GetAuth():

    session = boto3.Session(
        aws_access_key_id=config.access_id,
        aws_secret_access_key=config.access_key,
        region_name=config.aws_region
    )

    region = config.aws_region
    service = config.service
    credentials = session.get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    return awsauth


def main():

    Database = config.athena_db

    ret,athenaClient = GetAthenaConnObj()
    if 1 == ret:
        print('GetConnObj() Failed')
        return 1,''

    query = 'sql query'

    ret,response = RunQuery(athenaClient, Database, query)
    if 1 == ret:
        print('RunQuery() Failed')
        return 1,''

    return 0

if __name__ == '__main__':
    main()
