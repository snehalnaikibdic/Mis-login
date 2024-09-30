from fastapi import APIRouter, Request, Depends, HTTPException
from sqlalchemy.orm import Session
from models import GSPUserDetails, CorporateUserDetails, GSPAPIRequestLog
from database import get_db
from utils import AESCipher, validate_gst_pan, gsp_user_name_phone_no
from schemas import GSPUserCreateSchema
from dependencies import ErrorCodes
import logging
import requests
import json
import datetime

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/BankConsent/", description="API to fetch consent data from bank's page and store it in the database")
async def fetch_bank_consent(request: Request, db: Session = Depends(get_db)):
    try:
        header_dict = dict(request.headers)
        logger.info(f"Received headers: {header_dict}")

        data = await request.json()
        bank_api_url = data.get('bank_api_url')
        bank_request_data = data.get('bank_request_data')
        
        response = requests.post(bank_api_url, json=bank_request_data)
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch data from bank: {response.content}")
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch data from bank")

        consent_data = response.json()
        logger.info(f"Fetched consent data: {consent_data}")

        request_id = datetime.datetime.now().strftime("%Y%m%S%f")
        
        # Decrypt the data if necessary
        aes_key = header_dict.get('session-key', '')
        aes_iv = header_dict.get('session-iv', '')
        aes_encryption = AESCipher()
        raw_aes_key = aes_encryption.decrypt_key_using_private_key(aes_key)
        decrypted_consent_data = aes_encryption.aes_cbc_256_bit_decrypt(raw_aes_key, aes_iv, consent_data)
        decrypted_consent_data = json.loads(decrypted_consent_data)
        
        decrypted_consent_data.update({"requestId": data.get("requestId")})
        
        logger.info(f"Decrypted consent data: {decrypted_consent_data}")
        
        try:
            reg_pay_load = GSPUserCreateSchema(**decrypted_consent_data)
            request_data = jsonable_encoder(reg_pay_load)
        except HTTPException as httpExc:
            logger.error(f"HTTP Exception: {httpExc.detail}")
            return {"code": 400, "message": str(httpExc.detail)}
        except Exception as e:
            logger.error(f"Error: {e}")
            return {"code": 500, "message": str(e)}
        
        # Validate the GST and PAN details
        derived_pan_response = validate_gst_pan(request_data)
        logger.info(f"Derived PAN response: {derived_pan_response}")
        if derived_pan_response:
            return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1146)}
        
        # Store the consent data in the database
        gsp_user_req = db.query(GSPUserDetails).filter(
            GSPUserDetails.gstin == request_data.get('gstin'),
            GSPUserDetails.gsp == request_data.get('gsp'),
            GSPUserDetails.username == request_data.get('username'),
            GSPUserDetails.password == request_data.get('password')
        ).first()

        corporate_user_obj = db.query(CorporateUserDetails).filter(
            CorporateUserDetails.email_id == request_data.get('email')
        ).first()
        
        extra_data = {
            "requestId": request_data.get('requestId'),
            "ref": "bank_consent"
        }
        
        if not gsp_user_req:
            secret_key = request_data.get('gstin') + request_data.get('mobileNumber')
            encrypted_pass = aes_encryption.gsp_password_encryption(request_data.get('password'), secret_key)
        else:
            encrypted_pass = gsp_user_req.password
        
        if gsp_user_req:
            gsp_user_duplicate_response = gsp_user_name_phone_no(db, request_data)
            if gsp_user_duplicate_response.get("code") != 200:
                return {"requestId": request_data.get('requestId'), **gsp_user_duplicate_response}
            
            gsp_user_req.gsp = request_data.get('gsp')
            gsp_user_req.username = request_data.get('username')
            gsp_user_req.password = encrypted_pass
            gsp_user_req.name = request_data.get('name')
            gsp_user_req.pan = request_data.get('pan')
            gsp_user_req.email = request_data.get('emailId')
            gsp_user_req.mobile_number = request_data.get('mobileNumber')
            gsp_user_req.extra_data = extra_data
            gsp_user_req.created_by_id = corporate_user_obj.id if corporate_user_obj else None
            db.commit()
            db.refresh(gsp_user_req)
            return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1136)}
        else:
            gsp_user_duplicate_response = gsp_user_name_phone_no(db, request_data)
            if gsp_user_duplicate_response.get("code") != 200:
                return {"requestId": request_data.get('requestId'), **gsp_user_duplicate_response}
            
            gsp_gstin = db.query(GSPUserDetails).filter(GSPUserDetails.gstin == request_data.get('gstin')).first()
            if gsp_gstin and str(gsp_gstin.gstin) == request_data.get('gstin'):
                response_data = {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1149)}
                return response_data
            
            gsp_user_req_created = GSPUserDetails(
                gstin=request_data.get('gstin'),
                gsp=request_data.get('gsp'),
                username=request_data.get('username'),
                password=encrypted_pass,
                name=request_data.get('name'),
                pan=request_data.get('pan'),
                email=request_data.get('emailId'),
                mobile_number=request_data.get('mobileNumber'),
                extra_data=extra_data,
                created_by_id=corporate_user_obj.id if corporate_user_obj else None
            )
            db.add(gsp_user_req_created)
            db.commit()
            db.refresh(gsp_user_req_created)
            return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(200)}
    except Exception as e:
        logger.error(f"Error while fetching bank consent data: {e}")
        return {**ErrorCodes.get_error_response(500)}
