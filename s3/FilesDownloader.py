#-------------------------------------------------------------------------------
# Name:        s3download
# Purpose:
#
# Author:      abhishek.karkamkar
#
# Created:     08/04/2019
# Copyright:   (c) abhishek.karkamkar 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import boto3
import os

#Enter your ACCESS_KEY and ACCESS_ID
aws_region = "ap-south-1"
access_id = "ACCESS_ID"
access_key = "ACCESS_KEY"

def DownloadBucketFiles(s3Client, bucket, dwnfolderpath):
    try:
        response = s3Client.list_objects_v2(Bucket=bucket)
    except Exception as e:
        print("s3Client.list_objects_v2() raised exception => " + str(e))
        return 1

    for element in response['Contents']:
        filename = element['Key']
        fname = filename[filename.rfind('/')+1:]
        filepath = os.path.join(dwnfolderpath, fname)

        try:
            s3Client.download_file('test-edr-agni-1', element['Key'], filepath)
        except Exception as e:
            print("s3Client.download_file() raised exception => " + str(e))
            return 1

    return 0

def main():

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
        return 1

    # Download Files from bucket
    iret = DownloadBucketFiles(s3Client, "test-edr-agni-1", "F:\\aws\\s3\\0")
    if 0 == iret:
        print("DownloadBucketFiles() success")
        return 0
    else:
        print("DownloadBucketFiles() failed")
        return 1

    return 0


if __name__ == '__main__':
    main()
