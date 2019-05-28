import boto3
import string
import random
import time

#
# Credentials
#
aws_region = "ap-south-1"
access_id = "ACCESS_ID"
access_key = "ACCESS_KEY"

def msg_generator():
    size = 6
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


def CreateQueue(QName, sqs):
    try:
        response = sqs.create_queue(
        QueueName= QName,
        Attributes={
            'FifoQueue': 'true',
            'ContentBasedDeduplication': 'true'
        }
        )
    except Exception as e:
        print("sqs.create_queue() raised exception =>"+str(e))
        return 1,None

    #print(response)
    return 0, response['QueueUrl']

def SendMessage(message , queue_url, sqs):
    # Send message to SQS queue
    #MessageDeduplicationId='1',
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=(
                message
            ),
            MessageGroupId='1'
        )
    except Exception as e:
        print("sqs.send_message() raised exception => "+str(e))
        return 1

    #print(response)
    return 0

def RecvDelMessage(queue_url, sqs):
    # Receive message from SQS queue
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url
        )
    except Exception as e:
        print("sqs.receive_message() raised exception => "+str(e))
        return 1

    if 'Messages' not in response:
        print("queue empty")
        return 0

    message = response['Messages'][0]

    print(message)

    receipt_handle = message['ReceiptHandle']

    # Delete received message from queue
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
    except Exception as e:
        print("sqs.receive_message() raised exception => "+str(e))
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
    except Exception as e:
        print("session.client('sqs') raised exception => "+str(e))
        return 1

    iret,queue_url = CreateQueue("testqueue.fifo", sqs)
    if 0 == iret and queue_url != None:
        print()
        #print("CreateQueue() success")
        #return 0
    else:
        print("CreateQueue() failed")
        return 1

    while True:
        message = msg_generator()
        iret = SendMessage(message, queue_url, sqs)
        if 0 == iret:
            print(message+"\n")
            #print("SendMessage()"+message+" success")
            #return 0
        else:
            print("SendMessage() failed")
            return 1
        time.sleep(1)

    return 0

if __name__ == '__main__':
    main()

