import boto3
import botocore
import logging

class DynamoDBService:

    def __init__(self, region='ca-central-1'):
        self.client = boto3.client('dynamodb', region_name=region)

    def create_user(self, item_dict):
        try:
            item = {}

            if item_dict['username']:
                item['username'] = {'S': str(item_dict['username'])}

            if item_dict['password']:
                item['password'] = {'S': str(item_dict['password'])}

            if item_dict['role']:
                item['role'] = {'S': str(item_dict['role'])}

            response = self.client.put_item(
                TableName='bco_users',
                Item=item
            )
            
            return str(item_dict['username'])

        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                logging.error(err)
                raise err

            return None

    def get_user(self, username=None, password=None):

        try:
            user_db = None
            
            response = self.client.query(
                TableName='bco_users',
                ExpressionAttributeValues={
                    ':v1': {
                        'S': username,
                    },
                },
                KeyConditionExpression='username = :v1',
            )
            print(response)

            if len(response['Items']) > 0:
                user_db = {
                    'username': response['Items'][0]['username']['S'],
                    'password': response['Items'][0]['password']['S'],
                    'role': response['Items'][0]['role']['S']
                }

            return user_db

        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err

            return False


    def get_lead_last_id(self):

        try:            
            last_id = 0
            
            list_leads = self.get_leads()
            if list_leads and len(list_leads) > 0:
                for item in list_leads:
                    current_id = item['id'] if item['id'] else 0
                    if int(current_id) > int(last_id):
                        last_id = int(current_id)

            # Print the last record
            if last_id:
                print(f"Last record: {last_id}")
            
            return last_id

        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))

            return 0


    def get_leads(self):

        try:
            leads_db = None
            
            response = self.client.scan(
                TableName='bco_leads'
            )
            
            if len(response['Items']) > 0:
                leads_db = []
                
                for item in response['Items']:
                    lead_db = {
                        'id': item['id']['N'] if item['id'] else 0,
                        'username': item['username']['S'] if 'username' in item else "",
                        'person_name': item['person_name']['S'] if 'person_name' in item else "",
                        'company_name': item['company_name']['S'] if 'company_name' in item else "",
                        'company_website': item['company_website']['S'] if 'company_website' in item else "",
                        'company_address': item['company_address']['S'] if 'company_address' in item else "",
                        'telephone_numbers': item['telephone_numbers']['S'] if 'telephone_numbers' in item else "",
                        'email_address': item['email_address']['S'] if 'email_address' in item else "",
                        'image_filename': item['image_filename']['S'] if 'image_filename' in item else "",
                        'image_filepath': item['image_filepath']['S'] if 'image_filepath' in item else "",
                    }
                    leads_db.append(lead_db)
                    
            return leads_db

        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err

            return False


    def get_lead(self, lead_id=None):

        try:
            lead_db = None
            
            response = self.client.query(
                TableName='bco_leads',
                ExpressionAttributeValues={
                    ':v1': {
                        'N': lead_id,
                    },
                },
                KeyConditionExpression='id = :v1',
            )
            print(response)

            if len(response['Items']) > 0:
                lead_db = {
                    'id': response['Items'][0]['id']['N'] if response['Items'][0]['id'] else 0,
                    'username': response['Items'][0]['username']['S'] if 'username' in response['Items'][0] else "",
                    'person_name': response['Items'][0]['person_name']['S'] if 'person_name' in response['Items'][0] else "",
                    'company_name': response['Items'][0]['company_name']['S'] if 'company_name' in response['Items'][0] else "",
                    'company_website': response['Items'][0]['company_website']['S'] if 'company_website' in response['Items'][0] else "",
                    'company_address': response['Items'][0]['company_address']['S'] if 'company_address' in response['Items'][0] else "",
                    'telephone_numbers': response['Items'][0]['telephone_numbers']['S'] if 'telephone_numbers' in response['Items'][0] else "",
                    'email_address': response['Items'][0]['email_address']['S'] if 'email_address' in response['Items'][0] else "",
                    'image_filename': response['Items'][0]['image_filename']['S'] if 'image_filename' in response['Items'][0] else "",
                    'image_filepath': response['Items'][0]['image_filepath']['S'] if 'image_filepath' in response['Items'][0] else "",
                }

            return lead_db

        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                raise err

            return False


    def create_lead(self, username, item_dict):
        try:
            last_id = self.get_lead_last_id()
            new_id = int(last_id) + 1

            item = {}
            item['id'] = {'N': str(new_id)}

            if username:
                item['username'] = {'S': str(username)}

            if item_dict['image_filename']:
                item['image_filename'] = {'S': str(item_dict['image_filename'])}

            if item_dict['image_filepath']:
                item['image_filepath'] = {'S': str(item_dict['image_filepath'])}

            if item_dict['person_name']:
                item['person_name'] = {'S': str(item_dict['person_name'])}

            if item_dict['email_address']:
                item['email_address'] = {'S': str(item_dict['email_address'])}

            if item_dict['company_name']:
                item['company_name'] = {'S': str(item_dict['company_name'])}

            if item_dict['company_website']:
                item['company_website'] = {'S': str(item_dict['company_website'])}

            if item_dict['company_address']:
                item['company_address'] = {'S': str(item_dict['company_address'])}

            if item_dict['telephone_numbers']:
                item['telephone_numbers'] = {'S': str(item_dict['telephone_numbers'])}

            response = self.client.put_item(
                TableName='bco_leads',
                Item=item
            )
            
            return new_id

        except botocore.exceptions.ClientError as err:
            print("Error ", err)
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                logging.error(err)
                raise err

            return None


    def update_lead(self, lead_id, item_dict):
        try:
            item_key = {
                'id': {'N': str(lead_id)}
            }
            
            update_expression = 'SET company_name = :new_company_name'
            if item_dict['person_name']:
                update_expression += ', person_name = :new_person_name'
            if item_dict['email_address']:
                update_expression += ', email_address = :new_email_address'
            if item_dict['company_website']:
                update_expression += ', company_website = :new_company_website'
            if item_dict['company_address']:
                update_expression += ', company_address = :new_company_address'
            if item_dict['telephone_numbers']:
                update_expression += ', telephone_numbers = :new_telephone_numbers'
            
            expression_attribute_values = {}
            if item_dict['company_name']:
                expression_attribute_values[':new_company_name'] = {'S': str(item_dict['company_name'])}
            if item_dict['person_name']:
                expression_attribute_values[':new_person_name'] = {'S': str(item_dict['person_name'])}
            if item_dict['email_address']:
                expression_attribute_values[':new_email_address'] = {'S': str(item_dict['email_address'])}
            if item_dict['company_website']:
                expression_attribute_values[':new_company_website'] = {'S': str(item_dict['company_website'])}
            if item_dict['company_address']:
                expression_attribute_values[':new_company_address'] = {'S': str(item_dict['company_address'])}
            if item_dict['telephone_numbers']:
                expression_attribute_values[':new_telephone_numbers'] = {'S': str(item_dict['telephone_numbers'])}
            
            
            response = self.client.update_item(
                TableName='bco_leads',
                Key=item_key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values
            )
            print(response)
            
            return True

        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                logging.error(err)
                raise err

            return None


    def delete_lead(self, lead_id):
        try:
            item_key = {
                'id': {'N': str(lead_id)}
            }
            
            response = self.client.delete_item(
                TableName='bco_leads',
                Key=item_key
            )
            
            return True

        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == 'InternalError': # Generic error
                logging.warning('Error Message: {}'.format(err))
            else:
                logging.error(err)
                raise err

            return None

