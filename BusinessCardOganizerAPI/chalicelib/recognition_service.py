import boto3
import botocore
import logging


class RecognitionService:
    def __init__(self, storage_service):
        self.client = boto3.client('rekognition')
        self.bucket_name = storage_service.get_storage_location()

    def detect_text(self, file_name):
        
        try:
            response = self.client.detect_text(
                Image = {
                    'S3Object': {
                        'Bucket': self.bucket_name,
                        'Name': file_name
                    }
                }
            )
            print(response)

            lines = []
            for detection in response['TextDetections']:
                if detection['Type'] == 'LINE':
                    lines.append({
                        'text': detection['DetectedText'],
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

