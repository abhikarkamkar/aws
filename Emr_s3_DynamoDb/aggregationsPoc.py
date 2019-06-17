#-------------------------------------------------------------------------------
# Name:        aggregationPoc.py
# Purpose:
#
# Author:      abhishek.karkamkar
#
# Created:     08/04/2019
# Copyright:   (c) abhishek.karkamkar 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import boto3

#Enter your ACCESS_KEY and ACCESS_ID
aws_region = "ap-south-1"
access_id = "ACCESS_KEY"
access_key = "ACCESS_ID"

def PrepareItem(filedata, type):
    try:
        filedata = filedata.split('\n')
        print(filedata)
        filedata.remove('')
        data = []
        for ele in filedata:
            data.append(ele.replace('\x01',','))

        data.append('type,'+str(type))
        finaldata = {}
        temp = {}
        for ele in data:
            ele = ele.split(',')
            temp[ele[0]] = {}
            temp[ele[0]]['S'] = ele[1]
            finaldata.update(temp)
            temp.clear()
    except Exception as e:
        print("exception => "+str(e))
        return 1,''

    return 0,finaldata

def GetFileData(s3Client, bucket, key):
    try:
        response = s3Client.get_object(
                Bucket=bucket,
                Key=key
                )
    except Exception as e:
        print("s3Client.get_object raised exception => "+str(e))
        return 1,''

    try:
        res = response['Body'].read().decode('utf-8')
    except Exception as e:
        print("response['Body'].read().decode('utf-8') raised exception => "+str(e))
        return 1,''

    return 0,res


def InsertToDb(dbClient, table, item):
    try:
        response = dbClient.put_item(
            TableName=table,
            Item=item
            )
    except Exception as e:
        print("dbClient.put_item raised exception => "+str(e))
        return 1

    return 0

def DeleteS3Files(s3Client, Bucket, agg_dict):
    for type, filekey in agg_dict.items():
        try:
            s3Client.delete_object(
                Bucket=Bucket,
                Key=filekey
            )
        except Exception as e:
            print("s3Client.delete_object() raised exception => " + str(e))
            return 1

    return 0



def main():

    cluster_id = 'j-3I1CCFAF0FGXS'
    Bucket = 'test-edr-agni-1'
    Key = 'test/hive'
    Table = 'aggregations'

    #####################################################################################
    # Get Connection obj
    # +
    session = boto3.Session(
    aws_access_key_id=access_id,
    aws_secret_access_key=access_key,
    region_name=aws_region
    )

    # Create emr client
    try:
        emrClient = session.client('emr')
    except Exception as e:
        print("session.client('emr') raised exception => "+str(e))
        return 1

    # Create s3 client
    try:
        s3Client = session.client('s3')
    except Exception as e:
        print("session.client('s3') raised exception => "+str(e))
        return 1

    # Create dynamodb client
    try:
        dbClient = session.client('dynamodb')
    except Exception as e:
        print("session.client('dynamodb') raised exception => "+str(e))
        return 1

    # -
    # Get Connection obj
    #####################################################################################

    #####################################################################################
    # Prepare aggregation list
    # +

    agg_dict = {}
    agg_dict['status'] =  Key+'/status/000000_0'
    agg_dict['hostname'] = Key+'/hostname/000000_0'
    agg_dict['severity'] = Key+'/severity/000000_0'
    agg_dict['action'] = Key+'/action/000000_0'
    agg_dict['process'] = Key + '/process/000000_0'

    # -
    # Prepare aggregation list
    #####################################################################################

    #####################################################################################
    # Run hive job
    # +

    hive_args = "hive -f s3://test-edr-agni-1/test/aggregationshive.sql"
    hive_args_list = hive_args.split()

    try:
        response = emrClient.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    'Name': 'Hive_EMR_Step',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': hive_args_list
                    }
                },
            ]
        )
    except Exception as e:
        print("emrClient.add_job_flow_steps() raised exception => "+str(e))
        return 1

    print(response)
    stepid = response['StepIds'][0]

    try:
        waiter = emrClient.get_waiter("step_complete")
    except Exception as e:
        print("emrClient.get_waiter(step_complete) raised exception => "+str(e))
        return 1

    try:
        waiter.wait(
            ClusterId=cluster_id,
            StepId=stepid,
            WaiterConfig={
                "Delay": 60,
                "MaxAttempts": 123
            }
        )
    except Exception as e:
        print("emrClient waiter.wait() raised exception => "+str(e))
        return 1

    print("emr-hive step completed")

    # -
    # Run hive job
    #####################################################################################


    #####################################################################################
    # Prepare item and insert to Db
    # +

    items = []
    for type,filekey in agg_dict.items():
        # read s3 file data
        ret,filedata = GetFileData(s3Client, Bucket, filekey)
        if 1 == ret:
            print("GetFileData() Failed")
            return 1

        # prepare item to insert in db
        ret,item = PrepareItem(filedata,type)
        if 1 == ret:
            print("PrepareItem() Failed")
            return 1

        items.append(item)

    for item in items:
        # insert item in db
        ret = InsertToDb(dbClient, Table, item)
        if 1 == ret:
            print("InsertToDb() Failed")
            return 1

    # -
    # Prepare item and insert to Db
    #####################################################################################

    #####################################################################################
    # Delete files in s3
    # +

    ret = DeleteS3Files(s3Client, Bucket, agg_dict)
    if 1 == ret:
        print("DeleteS3Files() Failed")
        return 1

    # -
    # Delete files in s3
    #####################################################################################

    return 0

if __name__ == '__main__':
    main()
