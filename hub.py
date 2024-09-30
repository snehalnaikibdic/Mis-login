import json
import logging
import traceback
import copy

import requests
import redis

from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import desc

import config
import utils

import views
from database import SessionLocal
from sqlalchemy.orm import Session
from decouple import config as dconfig

from errors import ErrorCodes
from finance_view import AsyncFinancing, AsyncDisbursingFund, AsyncRepayment, webhook_finance_data
from models import MerchantDetails, HubRequestLog, Hub, PostProcessingRequest
from routers.auth import get_current_merchant_active_user, User
from routers.enquiry import cancel_fund_ledger, check_enquiry_api, async_validation_service_with_code_api, \
    async_validation_service_without_code_api, sync_validation_service_with_code, sync_validation_service_without_code
from routers.finance import sync_finance, async_finance, async_disbursement, async_repayment, sync_repayment
from routers.registration import async_registration_without_code, async_entity_registration, \
    async_invoice_registration_with_code, sync_invoice_registration_with_code, sync_registration_without_code, \
    sync_entity_registration
from routers.status_check import ledger_status_check, invoice_status_check_with_code, invoice_status_check_without_code, \
    sync_ledger_status_check, sync_invoice_status_check_with_code, sync_invoice_status_check_without_code
from schema import AsyncFinanceSchema, AsyncDisburseSchema, AsyncRepaymentSchema, HubSchema, CancelLedgerSchema, \
    InvoiceRequestSchema, EntityRegistrationSchema, AsyncInvoiceRegistrationWithCodeSchema, CheckEnquirySchema, \
    CheckStatusSchema, CheckInvoiceStatusSchemaWithCode, CheckInvoiceStatusSchemaWithoutCode
from utils import get_financial_year, check_invoice_date, create_post_processing, validate_signature, decrypt_aes_256, \
    encrypt_aes_256, validate_hub_signature, generate_aes_key

from database import get_db
import redis.asyncio as redis
from fastapi import Depends, FastAPI
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter


logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)
__RATE_LIMITER_TIMES = int(dconfig('RATE_LIMITER_TIMES'))
__RATE_LIMITER_SECONDS = int(dconfig('RATE_LIMITER_SECONDS'))


class APIEndPoint:
    syncFinancing = 'syncFinancing'
    syncDisbursement = 'syncDisbursement'
    syncRepayment = 'syncRepayment'
    asyncFinancing = 'asyncFinancing'
    asyncDisbursement = 'asyncDisbursement'
    asyncRepayment = 'asyncRepayment'
    cancel = 'cancel'
    async_registration_without_code_api = 'async-registration-without-code'
    async_entity_registration_api = 'async-entity-registration'
    async_invoice_registration_with_code_api = 'async-invoice-registration-with-code'
    sync_invoice_registration_with_code_api = 'sync-invoice-registration-with-code'
    sync_registration_without_code_api = 'sync-registration-without-code'
    sync_entity_registration_api = 'sync-entity-registration'
    enquiry = 'enquiry'
    validation_service_with_code = 'validation-service-with-code'
    validation_service_without_code = 'validation-service-without-code'
    sync_validation_service_with_code_api = 'sync-validation-service-with-code'
    sync_validation_service_without_code_api = 'sync-validation-service-without-code'
    async_ledger_status_check = 'async-ledger-status-check'
    async_invoice_status_check_with_code = 'async-invoice-status-check-with-code'
    async_invoice_status_check_without_code = 'async-invoice-status-check-without-code'
    sync_ledger_status_check_api = 'sync-ledger-status-check'
    sync_invoice_status_check_with_code_api = 'sync-invoice-status-check-with-code'
    sync_invoice_status_check_without_code_api = 'sync-invoice-status-check-without-code'

    end_point = {
        syncFinancing: 'syncFinancing',
        syncDisbursement: 'syncDisbursement',
        syncRepayment: 'syncRepayment',
        asyncFinancing: 'asyncFinancing',
        asyncDisbursement: 'asyncDisbursement',
        asyncRepayment: 'asyncRepayment',
        cancel: 'cancel',
        async_registration_without_code_api: 'async-registration-without-code',
        async_entity_registration_api: 'async-entity-registration',
        async_invoice_registration_with_code_api : 'async-invoice-registration-with-code',
        sync_invoice_registration_with_code_api : 'sync-invoice-registration-with-code',
        sync_registration_without_code_api : 'sync-registration-without-code',
        sync_entity_registration_api : 'sync-entity-registration',
        enquiry: 'enquiry',
        validation_service_with_code: 'validation-service-with-code',
        validation_service_without_code: 'validation-service-without-code',
        sync_validation_service_with_code_api: 'sync-validation-service-with-code',
        sync_validation_service_without_code_api: 'sync-validation-service-without-code',
        async_ledger_status_check: 'async-ledger-status-check',
        async_invoice_status_check_with_code: 'async-invoice-status-check-with-code',
        async_invoice_status_check_without_code: 'async-invoice-status-check-without-code',
        sync_ledger_status_check_api: 'sync-ledger-status-check',
        sync_invoice_status_check_with_code_api: 'sync-invoice-status-check-with-code',
        sync_invoice_status_check_without_code_api: 'sync-invoice-status-check-without-code'
    }

    end_point_func_mapper = {
        syncFinancing: (sync_finance, AsyncFinanceSchema),
        syncDisbursement: (async_disbursement, AsyncDisburseSchema),
        syncRepayment: (sync_repayment, AsyncRepaymentSchema),
        asyncFinancing: (async_finance, AsyncFinanceSchema),
        asyncDisbursement: (async_disbursement, AsyncDisburseSchema),
        asyncRepayment: (async_repayment, AsyncRepaymentSchema),
        cancel: (cancel_fund_ledger, CancelLedgerSchema),
        async_registration_without_code_api: (async_registration_without_code, InvoiceRequestSchema),
        async_entity_registration_api: (async_entity_registration, EntityRegistrationSchema),
        async_invoice_registration_with_code_api: (async_invoice_registration_with_code, AsyncInvoiceRegistrationWithCodeSchema),
        sync_invoice_registration_with_code_api: (sync_invoice_registration_with_code, AsyncInvoiceRegistrationWithCodeSchema),
        sync_registration_without_code_api: (sync_registration_without_code, InvoiceRequestSchema),
        sync_entity_registration_api: (sync_entity_registration, EntityRegistrationSchema),
        enquiry:(check_enquiry_api, CheckEnquirySchema),
        validation_service_with_code: (async_validation_service_with_code_api, AsyncInvoiceRegistrationWithCodeSchema),
        validation_service_without_code: (async_validation_service_without_code_api, InvoiceRequestSchema),
        sync_validation_service_with_code_api: (sync_validation_service_with_code, AsyncInvoiceRegistrationWithCodeSchema),
        sync_validation_service_without_code_api: (sync_validation_service_without_code, InvoiceRequestSchema),
        async_ledger_status_check: (ledger_status_check, CheckStatusSchema),
        async_invoice_status_check_with_code: (invoice_status_check_with_code, CheckInvoiceStatusSchemaWithCode),
        async_invoice_status_check_without_code:(invoice_status_check_without_code, CheckInvoiceStatusSchemaWithoutCode),
        sync_ledger_status_check_api:(sync_ledger_status_check, CheckStatusSchema),
        sync_invoice_status_check_with_code_api: (sync_invoice_status_check_with_code, CheckInvoiceStatusSchemaWithCode),
        sync_invoice_status_check_without_code_api: (sync_invoice_status_check_without_code, CheckInvoiceStatusSchemaWithoutCode)
    }

    @staticmethod
    def get_api_end_point(api_name):
        return APIEndPoint.end_point.get(api_name) or None
        # if api_name:
        #     return api_name
        # raise 'end point not found'


@router.post("/{url_end_point}/{merchant_key}/{hub_key}",
             response_description=None,
             dependencies=[Depends(RateLimiter(times=__RATE_LIMITER_TIMES, seconds=__RATE_LIMITER_SECONDS))])
def hub_route(url_end_point: str, merchant_key: str, hub_key: str, request: HubSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    try:
        url_end_point = APIEndPoint.get_api_end_point(url_end_point)
        if not url_end_point:
            return {"detail": "Not Found"}

        hub_obj = db.query(Hub).filter(Hub.hub_key == hub_key.strip()).first()
        if not hub_obj:
            return {
                **ErrorCodes.get_error_response(1071)
            }
        merchant_details_obj = db.query(MerchantDetails).filter(
            MerchantDetails.merchant_key == merchant_key
        ).filter(
            MerchantDetails.hub_id == hub_obj.id
        ).first()
        if not merchant_details_obj:
            return {
                **ErrorCodes.get_error_response(1071)
            }

        hub_request_log_res = utils.create_hub_request_log(db, request_data.get('correlationId'), request_data, '', 'request', str(url_end_point), hub_obj.id, merchant_details_obj.id)
        if hub_request_log_res.get("code") != 200:
             return  {
                "encryptData": "",
                "txnCode": request_data.get('txnCode'),
                "correlationId": request_data.get('correlationId'),
                "signature": request_data.get('signature'),
                "errorMessage": hub_request_log_res.get('message', "")
            }

        validate_signature_response = validate_hub_signature(db, request_data, hub_key)
        if validate_signature_response.get('code') != 200:
            validate_signature_response = {
                "encryptData": "",
                "txnCode": request_data.get('txnCode'),
                "correlationId": request_data.get('correlationId'),
                "signature": request_data.get('signature'),
                "errorMessage": validate_signature_response.get('message', "")
            }
            # create response log
            utils.create_hub_request_log(db, request.correlationId, '', validate_signature_response, 'response')
            return validate_signature_response

        key = generate_aes_key(str(merchant_details_obj.merchant_secret))
        try:
            pay_load = decrypt_aes_256(key, request_data.get('encryptData'))
        except Exception as e:
            return {
                "encryptData": "",
                "txnCode": request_data.get('txnCode'),
                "correlationId": request_data.get('correlationId'),
                "signature": request_data.get('signature'),
                "errorMessage": "Invalid encryptData"
            }

        schema_exception = ""
        try:
            pay_load = APIEndPoint.end_point_func_mapper.get(url_end_point)[1](**pay_load)
        except HTTPException as httpExc:
            logger.info(f"httpExc:: {httpExc.detail}")
            schema_exception = {"error": str(httpExc.detail)}
        except Exception as e:
            logger.info(f" Exception:: {e}")
            schema_exception = {"error": str(e)}

        if schema_exception:
            encrypt_api_resp = encrypt_aes_256(key, schema_exception)
            final_resp = {
                "encryptData": encrypt_api_resp,
                "txnCode": request_data.get('txnCode'),
                "correlationId": request_data.get('correlationId'),
                "signature": request_data.get('signature')
            }

            # update hub req log with api resp
            utils.create_hub_request_log(db, request_data.get('correlationId'), '', final_resp, 'response', '')
            return final_resp

        api_response = APIEndPoint.end_point_func_mapper.get(url_end_point)[0](merchant_key, pay_load, db)
        encrypt_api_resp = encrypt_aes_256(key, api_response)

        final_resp = {
            "encryptData": encrypt_api_resp,
            "txnCode": request_data.get('txnCode'),
            "correlationId": request_data.get('correlationId'),
            "signature": request_data.get('signature')
        }

        post_pro_req_obj = (
            db.query(
                PostProcessingRequest
            ).filter(
                PostProcessingRequest.request_extra_data.contains({"requestId": pay_load.requestId})
            ).order_by(
                desc(
                    PostProcessingRequest.id
                )
            ).first()
        )
        if post_pro_req_obj:
            logger.info(f"PostProcessingRequest object :: {post_pro_req_obj.id}")
            if post_pro_req_obj.extra_data:
                data = copy.deepcopy(post_pro_req_obj.extra_data)
                data.update({
                    "txnCode": request_data.get('txnCode'),
                    "correlationId": request_data.get('correlationId')
                })
            else:
                data = {
                    "txnCode": request_data.get('txnCode'),
                    "correlationId": request_data.get('correlationId')
                }
            post_pro_req_obj.extra_data = data
            db.commit()
            db.refresh(post_pro_req_obj)

        # update hub req log with api resp
        utils.create_hub_request_log(db, request_data.get('correlationId'), '', final_resp, 'response', '')
        return final_resp
    except Exception as e:
        logger.error(f"Error in sync_finance {e}")
        logger.error(f"Error in sync_finance {traceback.format_exc()}")
        return {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(500)
        }

