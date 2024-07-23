import boto3
import botocore
import logging


class TextractService:
    def __init__(self, storage_service):
        self.client = boto3.client('textract')
        self.bucket_name = storage_service.get_storage_location()

    def detect_text(self, file_name):
        try:
            print("detect text of textract")
            response = self.client.detect_document_text(
                Document = {
                    'S3Object': {
                        'Bucket': self.bucket_name,
                        'Name': file_name
                    }
                }
            )

            lines = []
            for detection in response['Blocks']:
                if detection['BlockType'] == 'LINE':
                    lines.append({
                        'text': detection['Text'],
                        'confidence': detection['Confidence'],
                        'boundingBox': detection['Geometry']['BoundingBox']
                    })
            return lines
            
        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err
            
            return False

