import asyncio
import time
import logging

import schemas
import models
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse

import utils
from database import get_db
from errors import ErrorCodes
from routers.auth import get_password_hash
from schema import InvoiceRequestSchema, EntityRegistrationSchema, AsyncInvoiceRegistrationWithCodeSchema, \
    CheckStatusSchema, CheckInvoiceStatusSchemaWithCode, CheckInvoiceStatusSchemaWithoutCode
from status_check_view import LedgerStatusCheck, InvoiceStatusCheckWithCode, InvoiceStatusCheckWithoutCode
from utils import generate_key_secret, create_post_processing, validate_signature
from models import MerchantDetails
from registration_view import AsyncInvoiceRegistrationCode, AsyncEntityRegistration, AsyncRegistration
from config import webhook_task

logger = logging.getLogger(__name__)
router = APIRouter()


class TagsMetadata:
    async_ledger_status_check_code = {
        "name": "Async Ledger Status Check Code",
        "description": "**Aim** : The ledger's status(funded/non-funded) can be checked using this API, and a response "
                       "can be received via a webhook.\n"
                       "\n**Validation** : \n"
                       "- RequestId format validation\n"
                       "- ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** : APIRequestLog, PostProcessingRequest, Ledger, MerchantDetails.\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, \n"
                       "ledger_status_check_post, check_ledger, ledger_webhook_data.",
        "summary": "Summary of asyncLedgerStatusCheckCode"
    }
    sync_ledger_status_check_code = {
        "name": "Async Ledger Status Check Code",
        "description": "**Aim** : The ledger's status(funded/non-funded) can be checked using this API, and a response "
                       "can be received via a webhook.\n"
                       "\n**Validation** : \n"
                       "- RequestId format validation\n"
                       "- ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** : APIRequestLog, Ledger, MerchantDetails\n"
                       "\n **Function** : create_request_log, validate_signature, \n"
                       "ledger_status_check_post, check_ledger.",
        "summary": "Summary of asyncLedgerStatusCheckCode"
    }
    async_invoice_status_check_withcode = {
        "name": "Async Invoice Status Check Code",
        "description": "**Aim** : This status API request will be serviced only if the requesting IDP has registered "
                       "the invoice with the entity code for which the status check(invoice funded/non founded) "
                       "is being requested. \n"
                       "\n**Validation** : \n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "- Invoice should have been registered by IDP previously\n"
                       "- Ledger should exist\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** : APIRequestLog, PostProcessingRequest, Invoice, MerchantDetails\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, \n"
                       "invoice_status_check_with_code_post, get_invoice_data_merchant, webhook_task. \n",
        "summary": "Summary of asyncInvoiceStatusCheckCode"
    }
    async_invoice_status_check_withoutcode = {
        "name": "Async Invoice Status Check without Code",
        "description": "**Aim** : This status API request will be serviced only if the requesting IDP has registered "
                       "the invoice does not required entity code for the status check(invoice funded/non "
                       "founded/self-funded)"
                       "is being requested. \n"
                       "\n**Validation** : \n"
                       "- RequestId format validation\n"
                       "- GST format validation\n"
                       "- Date format validation\n"
                       "- Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "- Invoice should have been registered by IDP previously\n"
                       "- Ledger should exist\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** : APIRequestLog, PostProcessingRequest, Invoice, MerchantDetails\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, \n"
                       " invoice_status_check_without_code_post, get_invoice_data, webhook_task. \n",
        "summary": "Summary of asyncInvoiceStatusCheckCode"
    }
    sync_invoice_status_check_withcode = {
        "name": "Sync Invoice Status Check Code",
        "description": "**Aim** : This status API request will be serviced only if the requesting IDP has registered "
                       "the invoice with the entity code for which the status check(invoice funded/non founded) "
                       "is being requested. \n"
                       "\n**Validation** : \n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "- Invoice should have been registered by IDP previously\n"
                       "- Ledger should exist\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** : APIRequestLog, Invoice, MerchantDetails\n"
                       "\n **Function** : create_request_log, validate_signature, \n"
                       "invoice_status_check_with_code_post. \n",
        "summary": "Summary of syncInvoiceStatusCheckCode"
    }
    sync_invoice_status_check_withoutcode = {
        "name": "Sync Invoice Status Check without Code",
        "description": "**Aim** : This status API request will be serviced only if the requesting IDP has registered "
                       "the invoice does not required entity code for the status check(invoice funded/non "
                       "founded/self-funded)"
                       "is being requested. \n"
                       "\n**Validation** : \n"
                       "- RequestId format validation\n"
                       "- GST format validation\n"
                       "- Date format validation\n"
                       "- Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "- Invoice should have been registered by IDP previously\n"
                       "- Ledger should exist\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** : APIRequestLog, Invoice, MerchantDetails\n"
                       "\n **Function** : create_request_log, validate_signature, \n"
                       " invoice_status_check_without_code_post, get_invoice_data. \n",
        "summary": "Summary of syncInvoiceStatusCheckCode"
    }


@router.post("/async-ledger-status-check/{merchant_key}",
             description=TagsMetadata.async_ledger_status_check_code.get('description'))
def ledger_status_check(merchant_key: str, request: CheckStatusSchema, db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'async-ledger-status-check',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = create_post_processing(
            db,
            jsonable_encoder(request),
            "ledger_status_check",
            "request",
            merchant_key,
            "successfully added in post processing"
        )
        return_response.update({"ledgerNo": request.ledgerNo})
        webhook_task.delay(jsonable_encoder(request), merchant_key, 'ledger_status_check')
    else:
        return_response = validate_signature_response
    utils.create_request_log(
        db,
        request.requestId,
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/async-invoice-status-check-with-code/{merchant_key}",
             description=TagsMetadata.async_invoice_status_check_withcode.get('description'))
def invoice_status_check_with_code(merchant_key: str, request: CheckInvoiceStatusSchemaWithCode,
                                   db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'async-invoice-status-check-with-code',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        validate_entity_code_response = validate_seller_buyer_code(
            jsonable_encoder(request),
            validate_signature_response.get('merchant_details'),
            db
        )
        if validate_entity_code_response.get('code') == 200:

            return_response = create_post_processing(
                db,
                jsonable_encoder(request),
                "invoice_status_check_with_code",
                "request",
                merchant_key,
                "successfully added in post processing"
            )
            webhook_task.delay(jsonable_encoder(request), merchant_key, 'invoice_status_check_with_code')
        else:
            return_response = validate_entity_code_response
    else:
        return_response = validate_signature_response
    utils.create_request_log(
        db,
        request.requestId,
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/async-invoice-status-check-without-code/{merchant_key}",
             description=TagsMetadata.async_invoice_status_check_withoutcode.get('description'))
def invoice_status_check_without_code(merchant_key: str, request: CheckInvoiceStatusSchemaWithoutCode,
                                      db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'async-invoice-status-check-without-code',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = create_post_processing(
            db,
            jsonable_encoder(request),
            "invoice_status_check_without_code",
            "request",
            merchant_key,
            "successfully added in post processing"
        )
        webhook_task.delay(jsonable_encoder(request), merchant_key, 'invoice_status_check_without_code')
    else:
        return_response = validate_signature_response
    utils.create_request_log(
        db,
        request.requestId,
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/sync-ledger-status-check/{merchant_key}",
             description=TagsMetadata.sync_ledger_status_check_code.get('description'))
def sync_ledger_status_check(merchant_key: str, request: CheckStatusSchema, db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'sync-ledger-status-check',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = LedgerStatusCheck.ledger_status_check_post(
            db,
            jsonable_encoder(request),
            merchant_key,
            'sync'
        )
        return_response.update({"ledgerNo": request.ledgerNo})
    else:
        return_response = validate_signature_response

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_request_log(db, request.requestId, '', return_response, 'response')
    utils.create_request_log(
        db,
        request.requestId,
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/sync-invoice-status-check-with-code/{merchant_key}",
             description=TagsMetadata.sync_invoice_status_check_withcode.get('description'))
def sync_invoice_status_check_with_code(merchant_key: str, request: CheckInvoiceStatusSchemaWithCode,
                                        db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'sync-invoice-status-check-with-code',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = InvoiceStatusCheckWithCode.invoice_status_check_with_code_post(
            db,
            jsonable_encoder(request),
            merchant_key,
            'sync'
        )
    else:
        return_response = validate_signature_response

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_request_log(db, request.requestId, '', return_response, 'response')
    utils.create_request_log(
        db,
        request.requestId,
        '',
        return_response,
        'response'
    )
    return return_response


def validate_seller_buyer_code(request_data, merchant_obj, db):
    seller_code = request_data.get('sellerCode')
    buyer_code = request_data.get('buyerCode')

    buyer_entity_combination_obj = db.query(
        models.EntityCombination
    ).filter(
        models.EntityCombination.merchant_id == merchant_obj.id,
        models.EntityCombination.entity_code == buyer_code
    ).first()

    seller_entity_combination_obj = db.query(
        models.EntityCombination
    ).filter(
        models.EntityCombination.merchant_id == merchant_obj.id,
        models.EntityCombination.entity_code == seller_code
    ).first()

    if not buyer_entity_combination_obj:
        return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1020)}

    if not seller_entity_combination_obj:
        return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1018)}

    return {"code": 200}


@router.post("/sync-invoice-status-check-without-code/{merchant_key}",
              description=TagsMetadata.sync_invoice_status_check_withoutcode.get('description'))
def sync_invoice_status_check_without_code(merchant_key: str, request: CheckInvoiceStatusSchemaWithoutCode,
                                           db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'sync-invoice-status-check-without-code',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = InvoiceStatusCheckWithoutCode.invoice_status_check_without_code_post(
            db,
            jsonable_encoder(request),
            merchant_key
        )
    else:
        return_response = validate_signature_response

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_request_log(db, request.requestId, '', return_response, 'response')
    utils.create_request_log(
        db,
        request.requestId,
        '',
        return_response,
        'response'
    )
    return return_response


