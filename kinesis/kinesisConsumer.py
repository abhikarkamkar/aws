import boto3
import time

def DescStream(stream, kinesis):
    try:
        response = kinesis.describe_stream(
            StreamName=stream
        )
    except Exception as e:
        print("kinesis.describe_stream() raised exception => "+str(e))
        return 1

    return response

def GetShardIterator(stream, kinesis, shardId):
    try:
        response = kinesis.get_shard_iterator(
            StreamName=stream,
            ShardId=shardId,
            ShardIteratorType='LATEST'
            #StartingSequenceNumber=startSeqNo,
        )
    except Exception as e:
        print("kinesis.get_shard_iterator() raised exception => " + str(e))
        return 1

    return response['ShardIterator']

def ConsumeData(stream, kinesis):

    response = DescStream(stream, kinesis)
    if response == 1:
        return 1

    if response['StreamDescription']['StreamStatus'] != 'ACTIVE':
        return 1

    shards = response['StreamDescription']['Shards']

    for shard in shards:
        shardId = shard['ShardId']
        shardItr = GetShardIterator(stream, kinesis, shardId)
        if 1 == shardItr:
            return 1

        startTime = time.time()
        try:
            response = kinesis.get_records(
                ShardIterator=shardItr,
                Limit=1
            )
        except Exception as e:
            print("kinesis.get_shard_iterator() raised exception => " + str(e))
            return 1

        endTime = 0
        while 'NextShardIterator' in response:

            if (endTime - startTime) >= 240:
                shardItr = GetShardIterator(stream, kinesis, shardId)
                if 1 == shardItr:
                    return 1
                startTime = time.time()

            response = kinesis.get_records(
                ShardIterator=response['NextShardIterator'],
                Limit=1
            )

            if len(response['Records']) > 0:
                print((response['Records'][0]['Data']).decode('ascii'))

            time.sleep(1)
            endTime = time.time()

    return 0


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


    iret = ConsumeData("teststream", kinesis)
    if 0 == iret:
        print("ConsumeData() success")
        #return 0
    else:
        print("ConsumeData() failed")
        return 1


    return 0

if __name__ == '__main__':
    main()
