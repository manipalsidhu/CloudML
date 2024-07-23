import boto3
import botocore
import logging


class ComprehendMedicalService:
    def __init__(self):
        self.client = boto3.client('comprehendmedical')

    def detect_entities(self, text_string):
        try:
            response = self.client.detect_entities_v2(
                Text = text_string
            )
            lines = []
            for entity in response['Entities']:
                lines.append({
                    'text': entity['Text'],
                    'confidence': entity['Score'] * 100,
                    'type': entity['Type'],
                    'category': entity['Category']
                })
            return lines
            
        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err
            
            return False

    def detect_phi(self, text_string):
        try:
            response = self.client.detect_phi(
                Text = text_string
            )
            lines = []
            for entity in response['Entities']:
                lines.append({
                    'text': entity['Text'],
                    'confidence': entity['Score'] * 100,
                    'type': entity['Type'],
                    'category': entity['Category']
                })
            return lines
            
        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err
            
            return False
