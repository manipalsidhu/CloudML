from chalice import Chalice
from chalicelib import storage_service
from chalicelib import recognition_service
from chalicelib import textract_service
from chalicelib import translation_service
from chalicelib import comprehend_service
from chalicelib import comprehend_medical_service
from chalicelib import dynamodb_service

import base64
import json
import logging

#####
# chalice app configuration
#####
app = Chalice(app_name='BusinessCardOganizerAPI')
app.debug = True

#####
# services initialization
#####
#storage_location = 'contentcen301218602.aws.ai'

storage_location = 'contentcentgroup5'
storage_service = storage_service.StorageService(storage_location)
recognition_service = recognition_service.RecognitionService(storage_service)
textract_service = textract_service.TextractService(storage_service)
translation_service = translation_service.TranslationService()
comprehend_service = comprehend_service.ComprehendService()
comprehend_medical_service = comprehend_medical_service.ComprehendMedicalService()
dynamodb_service = dynamodb_service.DynamoDBService()


#####
# RESTful endpoints
#####


@app.route('/v1/users', methods = ['POST'], cors = True)
def create_user():
    """User Creation using the DynamoDB Service"""

    json_response = {"status": "error", "msg": "User Creation Failed!", "lead_id": None}
        
    try:
        request_data = json.loads(app.current_request.raw_body)
        username = request_data['username']
        user_obj = request_data['user_obj']
        
        username_db = dynamodb_service.get_user(username)
        if username_db:
            if username_db["role"] == "admin":
                user_db = dynamodb_service.create_user(user_obj)
                if user_db:
                    json_response = {"status": "ok", "msg": "User Creation Successful!", "user_obj": user_db}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response
    
    
@app.route('/v1/login', methods = ['POST'], cors = True)
def login():
    """Processes login using DynamoDB service"""

    json_response = {"status": "error", "msg": "Login Failed!"}
        
    try:
        request_data = json.loads(app.current_request.raw_body)
        username = request_data['username']
        password = request_data['password']

        user_db = dynamodb_service.get_user(username)
        print(user_db)
        if user_db:
            if user_db["password"] == password and user_db["username"] == username:
                json_response = {"status": "ok", "msg": "Login Successful!", "role": user_db["role"]}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response


@app.route('/v1/leads', methods = ['GET'], cors = True)
def list_leads():
    """List Leads using the DynamoDB Service"""

    json_response = {"status": "error", "msg": "Leads Listing Failed!", "leads_obj": None}
        
    try:
        leads_db = dynamodb_service.get_leads()
        if leads_db:
            json_response = {"status": "ok", "msg": "Leads Listing Successful!", "leads_obj": leads_db}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response


@app.route('/v1/leads/lastid', methods = ['GET'], cors = True)
def get_lead_lastid():
    """Lead LastId using the DynamoDB Service"""

    json_response = {"status": "error", "msg": "Lead LastId Failed!", "lead_obj": None}
        
    try:
        lead_db = dynamodb_service.get_lead_last_id()
        if lead_db:
            json_response = {"status": "ok", "msg": "Lead LastId Successful!", "lead_obj": lead_db}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response


@app.route('/v1/leads/{lead_id}', methods = ['GET'], cors = True)
def get_lead(lead_id):
    """Lead Retrieval using the DynamoDB Service"""

    json_response = {"status": "error", "msg": "Lead Retrieval Failed!", "lead_obj": None}
        
    try:
        lead_db = dynamodb_service.get_lead(lead_id)
        if lead_db:
            json_response = {"status": "ok", "msg": "Lead Retrieval Successful!", "lead_obj": lead_db}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response


@app.route('/v1/leads', methods = ['POST'], cors = True)
def create_lead():
    """Lead Creation using the DynamoDB Service"""

    json_response = {"status": "error", "msg": "Lead Creation Failed!", "lead_id": None}
        
    try:
        request_data = json.loads(app.current_request.raw_body)
        username = request_data['username']
        business_card_obj = request_data['business_card_obj']
        
        lead_db = dynamodb_service.create_lead(username, business_card_obj)
        if lead_db:
            json_response = {"status": "ok", "msg": "Lead Creation Successful!", "lead_id": lead_db}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response


@app.route('/v1/leads/{lead_id}', methods = ['PUT'], cors = True)
def update_lead(lead_id):
    """Lead Update using the DynamoDB Service"""

    json_response = {"status": "error", "msg": "Lead Update Failed!"}
        
    try:
        request_data = json.loads(app.current_request.raw_body)
        username = request_data['username']
        business_card_obj = request_data['business_card_obj']
        
        lead_db = dynamodb_service.get_lead(lead_id)
        if lead_db:
            if username == lead_db["username"]:
                ## Delete company name in the item key
                lead_update_db = dynamodb_service.update_lead(lead_id, business_card_obj)
                if lead_update_db:
                    json_response = {"status": "ok", "msg": "Lead Update Successful!"}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response


@app.route('/v1/leads/{lead_id}', methods = ['DELETE'], cors = True)
def delete_lead(lead_id):
    """Lead Delete using the DynamoDB Service"""

    json_response = {"status": "error", "msg": "Lead Delete Failed!"}
        
    try:
        request_data = json.loads(app.current_request.raw_body)
        username = request_data['username']
        
        lead_db = dynamodb_service.get_lead(lead_id)
        if lead_db:
            if username == lead_db["username"]:
                ## Delete company name in the item key
                lead_delete_db = dynamodb_service.delete_lead(lead_id)
                if lead_delete_db:
                    json_response = {"status": "ok", "msg": "Lead Delete Successful!"}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response


@app.route('/v1/images', methods = ['POST'], cors = True)
def upload_image():
    """Processes file upload and saves file to storage service"""

    json_response = {"status": "error", "msg": "Image Upload Failed!", "image_obj": None}

    try:
        request_data = json.loads(app.current_request.raw_body)
        file_name = request_data['filename']
        file_bytes = base64.b64decode(request_data['filebytes'])

        image_info = storage_service.upload_file(file_bytes, file_name)
        if image_info:
            json_response = {"status": "ok", "msg": "Image Upload successfull!", "image_obj": image_info}

    except Exception as err:
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response


@app.route('/v1/images/{image_id}/extract_text', methods = ['POST'], cors = True)
def extract_text(image_id):
    """It then detects the text in the specified image using AWS Textract and Rekognition, 
    and tags the text to define business card attributes using AWS Comprehend and AWS Comprehend Medical"""

    json_response = {"status": "error", "msg": "Text Extraction / Tagging Failed!", "business_card_obj": None}
    
    dict_business_card_info = {
        'detected': [],
        'labeled': [],
        'extracted': {
            'image_filename': '',
            'person_name': '',
            'email_address': '',
            'company_name': '',
            'company_website': '',
            'company_address': '',
            'telephone_numbers': []
        }
    }            
            
    detected_lines= []
    labeled_lines = []
    text_lines_detected = []
    text_lines_labeled = []
    
    try:
        request_data = json.loads(app.current_request.raw_body)
        from_lang = request_data['fromLang']
        to_lang = request_data['toLang']

        MIN_CONFIDENCE = 60.0
        MIN_CONFIDENCE_COMPREHEND = 65.0

        #print("***************************** ")
        #print("****** TEXTRACT **** ")
        print("Textract")
        text_lines = textract_service.detect_text(image_id)
        for line in text_lines:            
            # check confidence
            if float(line['confidence']) >= MIN_CONFIDENCE and len(line['text']) >1:
                detected_lines.append({
                    'text': line['text'],
                    'boundingBox': line['boundingBox'],
                    'source': 'AWS Textract',
                })
                
                text_to_add = line['text']
                if text_to_add not in text_lines_detected:
                    text_lines_detected.append(text_to_add)
        print(text_lines_detected)
        
        #print("***************************** ")
        #print("****** RECOGNITIION **** ")
        print("RECOGNITIION")
        text_lines = recognition_service.detect_text(image_id)
        for line in text_lines:            
            # check confidence
            if float(line['confidence']) >= MIN_CONFIDENCE and len(line['text']) >1:
                detected_lines.append({
                    'text': line['text'],
                    'boundingBox': line['boundingBox'],
                    'source': 'AWS Rekognition',
                })
                
                text_to_add = line['text']
                if text_to_add not in text_lines_detected:
                    text_lines_detected.append(text_to_add)

        #print("***************************** ")
        
        if text_lines_detected and len(text_lines_detected) > 0:
            text_string_detected = ' | '.join(text_lines_detected)
            text_string_detected2 = ' '.join(text_lines_detected)
            text_string_detected = text_string_detected + " | " + text_string_detected2
            #print(text_string_detected)

            #print("***************************** ")
            #print("****** COMPREHEND MEDICAL  **** ")
            print("COMPREHEND MEDICAL")
            labeled_entities = comprehend_medical_service.detect_entities(text_string_detected)            
            for entity in labeled_entities:
                # check confidence                
                if float(entity['confidence']) >= MIN_CONFIDENCE_COMPREHEND and len(entity['text']) >1:
                    text_to_add = entity['text']
                    if text_to_add not in text_lines_labeled:
                        text_lines_labeled.append(text_to_add)

                        labeled_lines.append({
                            'text': entity['text'],
                            'type': entity['type'],
                            'confidence': entity['confidence'],
                            'source': 'AWS Comprehend Medical',
                        })
            
            #print("***************************** ")
            #print("****** COMPREHEND  **** ")
            print("COMPREHEND")
            labeled_entities = comprehend_service.detect_entities([text_string_detected], to_lang)
            for entity in labeled_entities:
                # check confidence
                if float(entity['confidence']) >= MIN_CONFIDENCE_COMPREHEND and len(entity['text']) >1:
                    text_to_add = entity['text']
                    if text_to_add not in text_lines_labeled:
                        text_lines_labeled.append(text_to_add)

                        labeled_lines.append({
                            'text': entity['text'],
                            'type': entity['type'],
                            'confidence': entity['confidence'],
                            'source': 'AWS Comprehend',
                        })

        dict_business_card_info['detected'] = detected_lines
        dict_business_card_info['labeled'] = labeled_lines
        
        boolAddress = False
        boolPhoneNumbers = False
        
        print("RESULTS")
        dict_business_card_info['extracted']['image_filename'] = image_id
        for labeled in labeled_lines:
            # Set Company Address Value
            if labeled['type'] == "ADDRESS":
                if dict_business_card_info['extracted']['company_address'] == '':
                    boolAddress = True
                    dict_business_card_info['extracted']['company_address'] = labeled['text']
            if labeled['type'] == "LOCATION":
                if dict_business_card_info['extracted']['company_address'] == '':
                    dict_business_card_info['extracted']['company_address'] = labeled['text']
                elif boolAddress == False:
                    if labeled['text'] not in dict_business_card_info['extracted']['company_address']:
                        dict_business_card_info['extracted']['company_address'] += ", " + labeled['text']

            # Set Company Website Value
            if labeled['type'] == "URL":
                if dict_business_card_info['extracted']['company_website'] == '':
                    dict_business_card_info['extracted']['company_website'] = labeled['text']

            # Set Company Name Value
            if labeled['type'] == "ORGANIZATION":
                if dict_business_card_info['extracted']['company_name'] == '':
                    dict_business_card_info['extracted']['company_name'] = labeled['text']
                else:
                    if len(dict_business_card_info['extracted']['company_name']) < 5:
                        if len(labeled['text']) > 5:
                            dict_business_card_info['extracted']['company_name'] = labeled['text']

            # Set Email Address Value
            if labeled['type'] == "EMAIL":
                if dict_business_card_info['extracted']['email_address'] == '':
                    dict_business_card_info['extracted']['email_address'] = labeled['text']

            # Set Person Name Value
            if labeled['type'] == "NAME":
                if dict_business_card_info['extracted']['person_name'] == '':
                    dict_business_card_info['extracted']['person_name'] = labeled['text']
            if labeled['type'] == "PERSON":
                if dict_business_card_info['extracted']['person_name'] == '':
                    dict_business_card_info['extracted']['person_name'] = labeled['text']

            # Set Telephone Numbers Value
            if labeled['type'] == "PHONE_OR_FAX":
                if labeled['text'] not in dict_business_card_info['extracted']['telephone_numbers']:
                    boolPhoneNumbers = True
                    dict_business_card_info['extracted']['telephone_numbers'].append(labeled['text'])
            
            # Set other values
            if labeled['type'] == "OTHER":
                # Set Telephone Numbers Value
                if boolPhoneNumbers == False:
                    if ("(" in labeled['text'] and ")" in labeled['text'] ) or "+" in labeled['text']:
                        if labeled['text'] not in dict_business_card_info['extracted']['telephone_numbers']:
                            dict_business_card_info['extracted']['telephone_numbers'].append(labeled['text'])
                
                # Set Email Address Value
                if dict_business_card_info['extracted']['email_address'] == '':
                    if "@" in labeled['text'] and "." in labeled['text']:
                        dict_business_card_info['extracted']['email_address'] = labeled['text']

        print("RESPONSE")
        json_response = {"status": "ok", "msg": "Text Extraction / Tagging Successful!", "business_card_obj": dict_business_card_info}

    except Exception as err: 
        print("Error ", err)
        logger.warning('Error Message: {}'.format(err))

    finally:
        return json_response
