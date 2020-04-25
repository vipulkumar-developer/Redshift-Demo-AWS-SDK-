import boto3
import logging
from botocore.exceptions import ClientError

class S3Manager:

    def __init__(self, region=None):
        if region is None:
            self.client = boto3.client('s3')
        else:
            self.client = boto3.client('s3', region_name=region)

    def createBucket(self, bucketName, region=None):
        try:
            if region is None:
                self.client.create_bucket(Bucket=bucketName)
            else:
                location = {'LocationConstraint': region}
                self.client.create_bucket(Bucket=bucketName,
                                    CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def getBuckets(self):
        return self.client.list_buckets()

    def uploadFile(self, fileName, bucket, objectName):
        try:
            response = self.client.upload_file(fileName, bucket, objectName)
        except ClientError as e:
            logging.error(e)
            return False
        return True