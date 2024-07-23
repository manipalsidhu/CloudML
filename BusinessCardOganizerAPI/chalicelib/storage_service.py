import boto3
import botocore
import logging


class StorageService:
    def __init__(self, storage_location):
        self.client = boto3.client('s3')
        self.bucket_name = storage_location

    def get_storage_location(self):
        return self.bucket_name

    def list_files(self):
        
        try:
            # Call API AWS to get a list of objects into the bucket s3
            # Documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2

            response = self.client.list_objects_v2(Bucket = self.bucket_name)

            files = []
            for content in response['Contents']:
                files.append({
                    'location': self.bucket_name,
                    'file_name': content['Key'],
                    'url': "http://" + self.bucket_name + ".s3.amazonaws.com/"
                        + content['Key']
                })
            return files
            
        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err
            
            return False

    def upload_file(self, file_bytes, file_name):
        
        try:
            self.client.put_object(Bucket = self.bucket_name,
                                Body = file_bytes,
                                Key = file_name,
                                ACL = 'public-read')

            return {'fileId': file_name,
                    'fileUrl': "http://" + self.bucket_name + ".s3.amazonaws.com/" + file_name}
            
        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err
            
            return False
