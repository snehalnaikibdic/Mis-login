import logging
import time
import redis
import ast
from typing import Annotated
from fastapi import APIRouter, Depends
from decouple import config as dconfig
from fastapi.encoders import jsonable_encoder

import models
from database import SessionLocal
from sqlalchemy.orm import Session
import utils
from models import MerchantDetails
from routers.auth import get_current_merchant_active_user, User
from schema import InvoiceRequestSchema, SFTPUserDetailSchema
import views
from errors import ErrorCodes
from utils import get_financial_year, check_invoice_date, check_invoice_due_date
from utils import generate_key_secret, create_post_processing, validate_signature
from config import webhook_task
from enquiry_view import AsyncValidationServiceWithCode, AsyncValidationServiceWithoutCode
from database import get_db
from sqlalchemy import desc, text
from models import MerchantDetails, APIRequestLog, SFTPUserInfo
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)


class TagsMetadata:
    sftp_info = {
        "name": "SFTP User Info",
        "description": "**Aim** : Create ITSM / SFTP User using API\n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- SFTP or ITSM based field validation\n"
                       "- In user type SFTP based Create and Delete\n"
                       "- In user type ITSM based Create, Update and Delete to be perform\n"
                       "- sftpUserBackendId is required to perform delete and update \n"
                       "\n **Table** : APIRequestLog, SFTPUserInfo\n"
                       "\n **Function** : create_request_log_sftpuser, create_sftp_user, \n",
        "summary": "Summary of SFTP User Info"
    }



def create_sftp_user(db, request):
    response_data = {**ErrorCodes.get_error_response(200)}
    actionType = request.get('action').capitalize()

    # if actionType == 'Create':
    #     if not request.get('sftpUserBackendId'):
    #         sftp_obj = db.query(models.SFTPUserInfo).filter(
    #             models.SFTPUserInfo.email_address == request.get('emailAddress')
    #         ).first()
    #         if not sftp_obj and actionType == 'Create':
    #             extra_data = {
    #                     "requestId": request.get('requestId'),
    #                     "idpId": request.get('idpId'),
    #                     "idpName": request.get('idpName'),
    #                     "SFTPUserId": request.get('sftpUserId'),
    #                     "SFTPUsername": request.get('sftpUsername'),
    #                     "SFTPPassword": request.get('sftpPassword'),
    #                     "ITSMUsername": request.get('itsmUsername'),
    #                     "ITSMPassword": request.get('itsmPassword'),
    #                     # "SFTPFirstname": request.get('sftpFirstname'),
    #                     # "customerName": request.get('customerName'),
    #                     "contactNumber": request.get('contactNumber'),
    #                     "userType": request.get('userType').upper(),
    #                     "project": request.get('project')
    #                 }
    #             sftp_user = models.SFTPUserInfo(
    #                     user_id=request.get('userId'),
    #                     role=request.get('role').strip(),
    #                     name=request.get('name').strip(),
    #                     email_address=request.get('emailAddress'),
    #                     extra_data=extra_data
    #                 )
    #             db.add(sftp_user)
    #             db.commit()
    #             db.refresh(sftp_user)
    #
    #             response_data.update({
    #                 'requestId': request.get('requestId'),
    #                 "sftpUserBackendId": str(sftp_user.id)
    #             })
    #             logger.info(f"new user create bcs pof action type- create{response_data}")
    #             return response_data
    #
    #         else:
    #             response_data = {
    #                 'requestId': request.get('requestId'),
    #                 **ErrorCodes.get_error_response(1107),
    #                 'userId': str(sftp_obj.user_id),
    #                 'sftpUserBackendId': str(sftp_obj.id)
    #             }
    #             return response_data
    #     else:
    #         response_data = {
    #             'requestId': request.get('requestId'),
    #             **ErrorCodes.get_error_response(1127)
    #         }
    #         return response_data
    # elif actionType == "Update":
    #     if not request.get('sftpUserBackendId'):
    #         response_data = {
    #             "requestId": request.get('requestId'),
    #             **ErrorCodes.get_error_response(1124)
    #         }
    #         return response_data
    #     sftp_user = db.query(models.SFTPUserInfo).filter(
    #         models.SFTPUserInfo.id == request.get('sftpUserBackendId')
    #     ).first()
    #
    #     if sftp_user and actionType == 'Update':
    #         sftp_email = db.query(models.SFTPUserInfo).filter(
    #             models.SFTPUserInfo.email_address == request.get('emailAddress')
    #         ).first()
    #         if sftp_email and str(sftp_email.id) != request.get('sftpUserBackendId'):
    #             response_data = {
    #                 'requestId': request.get('requestId'),
    #                 **ErrorCodes.get_error_response(1125),
    #                 'sftpUserBackendId': str(sftp_email.id),
    #             }
    #             return response_data
    #         elif sftp_email is None:
    #             extra_data = {
    #                 "requestId": request.get('requestId'),
    #                 "idpId": request.get('idpId'),
    #                 "idpName": request.get('idpName'),
    #                 "SFTPUserId": request.get('sftpUserId'),
    #                 "SFTPUsername": request.get('sftpUsername'),
    #                 "SFTPPassword": request.get('sftpPassword'),
    #                 "ITSMUsername": request.get('itsmUsername'),
    #                 "ITSMPassword": request.get('itsmPassword'),
    #                 "contactNumber": request.get('contactNumber'),
    #                 "userType": request.get('userType').upper(),
    #                 "project": request.get('project')
    #             }
    #             sftp_user.user_id = request.get('userId')
    #             sftp_user.role = request.get('role').strip()
    #             sftp_user.name = request.get('name').strip()
    #             sftp_user.email_address = request.get('emailAddress')
    #             sftp_user.extra_data = extra_data
    #
    #             db.commit()
    #             db.refresh(sftp_user)
    #             response_data = {
    #                 'requestId': request.get('requestId'),
    #                 **ErrorCodes.get_error_response(1122)
    #             }
    #             return response_data
    #         elif sftp_email.email_address == request.get('emailAddress') and str(sftp_email.id) == request.get('sftpUserBackendId'):
    #             extra_data = {
    #                 "requestId": request.get('requestId'),
    #                 "idpId": request.get('idpId'),
    #                 "idpName": request.get('idpName'),
    #                 "SFTPUserId": request.get('sftpUserId'),
    #                 "SFTPUsername": request.get('sftpUsername'),
    #                 "SFTPPassword": request.get('sftpPassword'),
    #                 "ITSMUsername": request.get('itsmUsername'),
    #                 "ITSMPassword": request.get('itsmPassword'),
    #                 # "SFTPFirstname": request.get('sftpFirstname'),
    #                 # "customerName": request.get('customerName'),
    #                 "contactNumber": request.get('contactNumber'),
    #                 "userType": request.get('userType').upper(),
    #                 "project": request.get('project')
    #             }
    #             sftp_user.user_id = request.get('userId')
    #             sftp_user.role = request.get('role').strip()
    #             sftp_user.name = request.get('name').strip()
    #             sftp_user.email_address = request.get('emailAddress')
    #             sftp_user.extra_data = extra_data
    #
    #             db.commit()
    #             db.refresh(sftp_user)
    #             response_data = {
    #                 'requestId': request.get('requestId'),
    #                 **ErrorCodes.get_error_response(1122)
    #             }
    #             return response_data
    #     else:
    #         response_data = {
    #             'requestId': request.get('requestId'),
    #             **ErrorCodes.get_error_response(1126),
    #         }
    #         return response_data
    # else:
    #     if not request.get('sftpUserBackendId'):
    #         response_data = {
    #             "requestId": request.get('requestId'),
    #             **ErrorCodes.get_error_response(1124)
    #         }
    #         return response_data
    #     sftp_user_del = db.query(models.SFTPUserInfo).filter(
    #         models.SFTPUserInfo.id == request.get('sftpUserBackendId')
    #     ).first()
    #     if sftp_user_del:
    #         db.delete(sftp_user_del)
    #         db.commit()
    #         response_data = {
    #             'requestId': request.get('requestId'),
    #             **ErrorCodes.get_error_response(1123)
    #         }
    #         return response_data
    #     else:
    #         response_data = {
    #             'requestId': request.get('requestId'),
    #             **ErrorCodes.get_error_response(1126),
    #         }
    #         return response_data
    #
    #     return response_data

# email address unquie is remove now
    if actionType == 'Create':
        if not request.get('sftpUserBackendId'):

            sftp_user_obj = db.query(
                models.SFTPUserInfo
            ).filter(
                models.SFTPUserInfo.extra_data.contains({"userType": request.get('userType').upper()})
            ).filter(
                models.SFTPUserInfo.email_address == request.get('emailAddress')
            ).first()

            if sftp_user_obj: #  and sftp_user_obj.extra_data.get('userType') in ['SFTP', 'ITSM']
                response_data = {
                    'requestId': request.get('requestId'),
                    **ErrorCodes.get_error_response(1133)
                }
                return response_data

            extra_data = {
                    "requestId": request.get('requestId'),
                    "idpId": request.get('idpId'),
                    "idpName": request.get('idpName'),
                    "SFTPUserId": request.get('sftpUserId'),
                    "SFTPUsername": request.get('sftpUsername'),
                    "SFTPPassword": request.get('sftpPassword'),
                    "ITSMUsername": request.get('itsmUsername'),
                    "ITSMPassword": request.get('itsmPassword'),
                    "contactNumber": request.get('contactNumber'),
                    "userType": request.get('userType').upper(),
                    "project": request.get('project')
                }
            sftp_user = models.SFTPUserInfo(
                    user_id=request.get('userId'),
                    role=request.get('role').strip(),
                    name=request.get('name').strip(),
                    email_address=request.get('emailAddress'),
                    extra_data=extra_data
                )
            db.add(sftp_user)
            db.commit()
            db.refresh(sftp_user)

            response_data.update({
                'requestId': request.get('requestId'),
                "sftpUserBackendId": str(sftp_user.id)
            })
            logger.info(f"new user create bcs pof action type- create{response_data}")
            return response_data
        else:
            response_data = {
                'requestId': request.get('requestId'),
                **ErrorCodes.get_error_response(1127)
            }
            return response_data
    elif actionType == "Update":
        if not request.get('sftpUserBackendId'):
            response_data = {
                "requestId": request.get('requestId'),
                **ErrorCodes.get_error_response(1124)
            }
            return response_data
        sftp_user = db.query(models.SFTPUserInfo).filter(
            models.SFTPUserInfo.id == request.get('sftpUserBackendId')
        ).first()

        if sftp_user and actionType == 'Update':
            extra_data = {
                "requestId": request.get('requestId'),
                "idpId": request.get('idpId'),
                "idpName": request.get('idpName'),
                "SFTPUserId": request.get('sftpUserId'),
                "SFTPUsername": request.get('sftpUsername'),
                "SFTPPassword": request.get('sftpPassword'),
                "ITSMUsername": request.get('itsmUsername'),
                "ITSMPassword": request.get('itsmPassword'),
                "contactNumber": request.get('contactNumber'),
                "userType": request.get('userType').upper(),
                "project": request.get('project')
            }
            sftp_user.user_id = request.get('userId')
            sftp_user.role = request.get('role').strip()
            sftp_user.name = request.get('name').strip()
            sftp_user.email_address = request.get('emailAddress')
            sftp_user.extra_data = extra_data

            db.commit()
            db.refresh(sftp_user)
            response_data = {
                'requestId': request.get('requestId'),
                **ErrorCodes.get_error_response(1122)
            }
            return response_data
        else:
            response_data = {
                'requestId': request.get('requestId'),
                **ErrorCodes.get_error_response(1126),
            }
            return response_data
    else:
        if not request.get('sftpUserBackendId'):
            response_data = {
                "requestId": request.get('requestId'),
                **ErrorCodes.get_error_response(1124)
            }
            return response_data
        sftp_user_del = db.query(models.SFTPUserInfo).filter(
            models.SFTPUserInfo.id == request.get('sftpUserBackendId')
        ).first()
        if sftp_user_del:
            db.delete(sftp_user_del)
            db.commit()
            response_data = {
                'requestId': request.get('requestId'),
                **ErrorCodes.get_error_response(1123)
            }
            return response_data
        else:
            response_data = {
                'requestId': request.get('requestId'),
                **ErrorCodes.get_error_response(1126),
            }
            return response_data


@router.post("/sftpUserInfo/", description=TagsMetadata.sftp_info.get('description'))
def sftp_user_info(request: SFTPUserDetailSchema, db: Session = Depends(get_db)):
    try:
        json_data = jsonable_encoder(request)
        response_data = {}
        # api request log
        api_request_log_res = views.create_request_log_sftpuser(
            db,
            request.requestId,
            jsonable_encoder(request),
            '',
            'request',
            'SFTP-user-info',
            ''
        )
        if api_request_log_res.get("code") != 200:
            return {
                "requestId": json_data.get('requestId'),
                **api_request_log_res
            }
        # actionType = json_data.get('action')
        # sftp_user_info_email = json_data.get('emailAddress')
        user_cr = create_sftp_user(db, json_data)
        response_data.update({**user_cr})

        return response_data
    except Exception as e:
        logger.error(f"getting error while sync validation service with code {e}")
        return {**ErrorCodes.get_error_response(500)}

