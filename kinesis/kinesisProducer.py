import boto3
import json
import string
import random
import time

def msg_generator():
    size = 6
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))

def DescStream(stream, kinesis):
    try:
        response = kinesis.describe_stream(
            StreamName=stream
        )
    except Exception as e:
        print("kinesis.describe_stream() raised exception => "+str(e))
        return 1

    return response

def CreateStream(stream, kinesis):
    try:
        response = kinesis.create_stream(
            StreamName=stream,
            ShardCount=1
        )
    except Exception as e:
        print("kinesis.create_stream() raised exception => "+str(e))
        return 1

    response = DescStream(stream, kinesis)
    if response == 1:
        return 1

    if response['StreamDescription']['StreamStatus'] == 'CREATING' or response['StreamDescription']['StreamStatus'] == 'ACTIVE':
        return 0
    else:
        print(response)
        return 1

def SendData(stream, kinesis, data):
    try:
        response = kinesis.put_record(
            StreamName=stream,
            Data=data,
            PartitionKey='1'
        )
    except Exception as e:
        print("kinesis.put_record() raised exception => "+str(e))
        return 1

    if 'ShardId' in response:
        return 0
    else:
        print(response)
        return 1

def main():

    session = boto3.Session(
    aws_access_key_id="ACCESS_KEY_ID",
    aws_secret_access_key="ACCESS_KEY",
    region_name="ap-south-1"
    )

    # Create KINESIS client
    try:
        kinesis = session.client('kinesis')
    except Exception as e:
        print("session.client('kinesis') raised exception => "+str(e))
        return 1

    #
    #create KINESIS stream
    #

    # iret = CreateStream("teststream", kinesis)
    # if 0 == iret:
    #     print("CreateStream() success")
    #     return 0
    # else:
    #     print("CreateStream() failed")
    #     return 1

    #
    #Send Data to KINESIS stream
    #
    while True:
        msg = msg_generator()
        data = {'testmsg':msg}
        data = json.dumps(data)

        iret = SendData("teststream", kinesis, data)
        if 0 == iret:
            print("SendData() "+msg+" success")
            #return 0
        else:
            print("SendData() failed")
            return 1

        time.sleep(1)

    return 0

if __name__ == '__main__':
    main()
