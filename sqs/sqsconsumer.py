import boto3
import time

#
# Credentials
#
aws_region = "ap-south-1"
access_id = "ACCESS_ID"
access_key = "ACCESS_KEY"


def GetQUrl(QName, sqs):
    try:
        response = sqs.get_queue_url(QueueName=QName)
    except:
        print("sqs.get_queue_url() raised exception")
        return 1

    return 0,response['QueueUrl']

def RecvDelMessage(queue_url, sqs):
    # Receive message from SQS queue
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url
        )
    except:
        print("sqs.receive_message() raised exception")
        return 1

    if 'Messages' not in response:
        print("queue empty")
        return 0

    message = response['Messages'][0]

    print(message['Body'])

    receipt_handle = message['ReceiptHandle']

    # Delete received message from queue
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
    except:
        print("sqs.receive_message() raised exception")
        return 1

    return 0

def main():

    session = boto3.Session(
    aws_access_key_id=access_id,
    aws_secret_access_key=access_key,
    region_name=aws_region
    )

    # Create SQS client
    try:
        sqs = session.client('sqs')
    except:
        print("session.client('sqs') raised exception")
        return 1

    iret,queue_url = GetQUrl("testqueue.fifo", sqs)
    if 0 == iret:
        print()
        #print("GetQUrl() success")
        #return 0
    else:
        print("GetQUrl() failed")
        return 1

    while True:
        iret = RecvDelMessage(queue_url, sqs)
        if 0 == iret:
            print()
            #print("RecvDelMessage() success")
            # return 0
        else:
            print("RecvDelMessage() failed")
            return 1
        time.sleep(1)

    return 0

if __name__ == '__main__':
    main()

