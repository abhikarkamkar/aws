#-------------------------------------------------------------------------------
# Name:        s3upload
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

def DirTraverse(dirPath, s3Client, bucket, folder):
    for root, dirs, files in os.walk(dirPath):
        for file in files:
            filepath = os.path.join(root,file)
            # Upload File
            iret = UploadFile(s3Client, bucket, folder, filepath, file)
            if 0 == iret:
                print("File {} uploaded".format(file))
                #return 0
            else:
                print("File {} upload failed".format(file))
                return 1

    return 0

def CreateBucket(s3Client, bucket):
    try:
        response = s3Client.create_bucket(
                            Bucket=bucket,
                            CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'}
                            )
    except Exception as e:
        print("s3Client.create_bucket() raised exception => " + str(e))
        return 1

    return 0


def UploadFile(s3Client, bucket, folder, filepath, filename):
    filename = folder+"/"+filename
    try:
        response = s3Client.upload_file(
                            filepath,
                            bucket,
                            filename
        )
    except Exception as e:
        print("s3Client.meta.client.upload_file() raised exception => " + str(e))
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

    # Create s3 bucket
    iret = CreateBucket(s3Client, "test-edr-agni-1")
    if 0 == iret:
         print("CreateBucket() success")
         return 0
    else:
         print("CreateBucket() failed")
         return 1


    # Upload Files from dir
    iret = DirTraverse("F:\\aws\\s3\\1", s3Client, "test-edr-agni-1", "test")
    if 0 == iret:
        print("DirTraverse() success")
        return 0
    else:
        print("DirTraverse() failed")
        return 1

    return 0


if __name__ == '__main__':
    main()
