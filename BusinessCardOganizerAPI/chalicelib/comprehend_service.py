import boto3
import botocore
import logging


class ComprehendService:
    def __init__(self):
        self.client = boto3.client('comprehend')

    def detect_entities(self, text_list, target_language = 'en'):
        try:
            response = self.client.batch_detect_entities(
                TextList = text_list,
                LanguageCode = target_language
            )
            lines = []
            for detection in response['ResultList']:
                for entity in detection['Entities']:
                    lines.append({
                        'text': entity['Text'],
                        'confidence': entity['Score']* 100,
                        'type': entity['Type']
                    })
            return lines
            
        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err
            
            return False
