import logging
import traceback

import redis
from typing import Annotated
from fastapi import APIRouter, Depends, Request
from fastapi.encoders import jsonable_encoder

import utils
import views
from database import SessionLocal
from sqlalchemy.orm import Session
from decouple import config as dconfig

from errors import ErrorCodes
from finance_view import AsyncFinancing, AsyncDisbursingFund, AsyncRepayment, webhook_finance_data
from models import MerchantDetails
from routers.auth import get_current_merchant_active_user, User
from schema import AsyncFinanceSchema, AsyncDisburseSchema, AsyncRepaymentSchema, BulkAsyncFinanceSchema
from utils import get_financial_year, check_invoice_date, create_post_processing, validate_signature
from config import webhook_task
from database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)


class TagsMetadata:
    async_financing = {
        "name": "asyncFinancing",
        "description": "**Aim** : Registered Invoices are eligible for financing and this api will update the status of ledger and invoices to funded.\n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- Ledger data for all the invoices in the ledger should be provided in the financing api\n"
                       "- Invoice amount should be the same as that provided in the registration api Adjustment amount should be less than or equal to invoice amount \n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** : APIRequestLog, PostProcessingRequest, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, \n"
                       "async_invoice_financing, finance_invoice, webhook_finance_data.",
        "summary": "Summary of asyncFinancing"
    }
    async_disbursement = {
        "name": "asyncDisbursement",
        "description": "**Aim** : Funded Invoices will be disbursed and Ledger and Invoice status marked to partial/full_disbursed.\n"
                       "\n**Validation** : \n"
                       "- Disbursed amount should be less than or equal to finance amount\n"
                       "- Disbursed date should be later than financed date & invoice date \n"
                       "- Due date should be later than disbursed date \n"
                       "\n **Table** : APIRequestLog, PostProcessingRequest, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, async_invoice_disbursing, \n"
                       "disbursing_invoice, disburse_ledger, check_invoice.",
        "summary": "async_disbursement summary ",
    }
    async_repayment = {
        "name": "asyncRepayment",
        "description": "**Aim** : On Recieving payment of Disbursed Invoices, this api will mark Ledger and Invoice status to repaid/partial_repaid.\n"
                       "\n**Validation** : \n"
                       "- requestId format validation\n"
                       "- Borrower Category format validation\n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "- Date format validation\n"
                       "\n**Table** :APIRequestLog, PostProcessingRequest, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n**Function** : create_request_log, validate_signature, create_post_processing, async_invoice_repayment, \n"
                       " repayment_invoice, repayment_ledger, repayment_ledger, check_invoice.",
        "summary": "async_repayment summary "
    }
    sync_financing = {
        "name": "syncFinancing",
        "description": "**Aim** : Registered Invoices are eligible for financing and this api will update the status of ledger and invoices to funded.\n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- Ledger data for all the invoices in the ledger should be provided in the financing api\n"
                       "- Invoice amount should be the same as that provided in the registration api Adjustment amount should be less than or equal to invoice amount \n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n**Table** :APIRequestLog, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n **Function** : create_request_log, validate_signature, \n"
                       "async_invoice_financing, finance_invoice.",
        "summary": "Summary of syncFinancing"
    }
    sync_disbursement = {
        "name": "syncDisbursement",
        "description": "**Aim** : Funded Invoices will be disbursed and Ledger and Invoice status marked to partial/full_disbursed.\n"
                       "\n**Validation** : \n"
                       "- Disbursed amount should be less than or equal to finance amount \n"
                       "- Disbursed date should be later than financed date & invoice date \n"
                       "- Due date should be later than disbursed date \n"
                       "\n**Table** :APIRequestLog, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n**Function** : create_request_log, validate_signature, disbursing_invoice, validate_ledger_date,\n"
                       "disbursing_invoice, disburse_ledger, check_invoice.",
        "summary": "sync_disbursement summary ",
    }
    sync_repayment = {
        "name": "syncRepayment",
        "description": "**Aim** : On Receiving payment of Disbursed Invoices, this api will mark Ledger and Invoice status to repaid/partial_repaid.\n"
                       "\n**Validation** : \n"
                       "- requestId format validation\n"
                       "- Borrower Category format validation\n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "- Date format validation\n"
                       "\n**Table** : APIRequestLog, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n**Function** : create_request_log, validate_signature,repayment_invoice,\n"
                       " repayment_ledger, check_ledger, check_invoice",
        "summary": "sync_repayment summary ",
        "externalDocs": {
            "description": "Items external docs",
            "url": "https://fastapi.tiangolo.com/",
        },
    }


@router.post("/asyncFinancing/{merchant_key}",
             description=TagsMetadata.async_financing.get('description'))
def async_finance(merchant_key: str, request: AsyncFinanceSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    api_request_log_res = views.create_request_log(db, request_data.get('requestId'), request_data, '', 'request', 'asyncFinancing', merchant_key)
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request_data.get('requestId'),
            **api_request_log_res
        }

    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') != 200:
        # create response log
        views.create_request_log(db, request.requestId, '', validate_signature_response, 'response')
        return validate_signature_response

    # request validation
    fin_req_val_resp = AsyncFinancing.fin_req_validation(request_data)
    if fin_req_val_resp.get('code') != 200:
        views.create_request_log(db, request.requestId, '', fin_req_val_resp, 'response')
        return fin_req_val_resp

    return_response = create_post_processing(
        db,
        request_data,
        "asyncFinancing",
        'request',
        merchant_key,
        "successfully added in post processing"
    )

    webhook_task.delay(
        request_data,
        merchant_key,
        'async_financing'
    )

    return return_response


@router.post("/asyncDisbursement/{merchant_key}",
             description=TagsMetadata.async_disbursement.get('description'))
def async_disbursement(merchant_key: str, request: AsyncDisburseSchema,db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    api_request_log_res = views.create_request_log(db, request_data.get('requestId'), request_data, '', 'request', 'asyncDisbursement', merchant_key)
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request_data.get('requestId'),
            **api_request_log_res
        }

    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') != 200:
        # create response log
        views.create_request_log(db, request.requestId, '', validate_signature_response, 'response')
        return validate_signature_response

    # validate ledger date
    val_date_resp = utils.validate_ledger_date(request_data)
    if val_date_resp.get('code') != 200:
        return val_date_resp

    return_response = create_post_processing(
        db,
        request_data,
        "asyncDisbursement",
        'request',
        merchant_key,
        "successfully added in post processing"
    )

    webhook_task.delay(
        request_data,
        merchant_key,
        'async_disbursement'
    )
    return return_response


@router.post("/asyncRepayment/{merchant_key}", description=TagsMetadata.async_repayment.get('description'))
def async_repayment(merchant_key: str, request: AsyncRepaymentSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    api_request_log_res = views.create_request_log(db, request_data.get('requestId'), request_data, '', 'request', 'asyncRepayment', merchant_key)
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request_data.get('requestId'),
            **api_request_log_res
        }

    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') != 200:
        # create response log
        views.create_request_log(db, request.requestId, '', validate_signature_response, 'response')
        return validate_signature_response

    # validate ledger date
    val_date_resp = utils.validate_ledger_date(request_data)
    if val_date_resp.get('code') != 200:
        return val_date_resp

    return_response = create_post_processing(
        db,
        request_data,
        "asyncRepayment",
        'request',
        merchant_key,
        "successfully added in post processing"
    )
    webhook_task.delay(
        request_data,
        merchant_key,
        'async_repayment'
    )

    return return_response


#################### synchronous manner finance #######################################

@router.post("/syncFinancing/{merchant_key}", description=TagsMetadata.sync_financing.get('description'))
def sync_finance(merchant_key: str, request: AsyncFinanceSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)

    try:
        api_request_log_res = views.create_request_log(db, request_data.get('requestId'), request_data, '', 'request', 'syncFinancing', merchant_key)
        if api_request_log_res.get("code") != 200:
            return {
                "requestId": request_data.get('requestId'),
                **api_request_log_res
            }

        validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
        if validate_signature_response.get('code') != 200:
            # create response log
            views.create_request_log(db, request_data.get('requestId'), '', validate_signature_response, 'response')
            return validate_signature_response

        # request validation
        fin_req_val_resp = AsyncFinancing.fin_req_validation(request_data)
        if fin_req_val_resp.get('code') != 200:
            views.create_request_log(db, request_data.get('requestId'), '', fin_req_val_resp, 'response')
            return fin_req_val_resp

        # call api
        api_response = AsyncFinancing.finance_invoice(db, request_data, merchant_key)
        merchant_obj = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()

        # if return_response.get('code') in [200, 1013, 1004]:
        webhook_req_data = webhook_finance_data(db, request_data, merchant_obj)
        webhook_request = {
            **api_response,
            **webhook_req_data
        }
        # else:
        #     webhook_request = {
        #         **return_response
        #     }

        webhook_response_hash = utils.create_response_hash(db, webhook_request, merchant_key)
        webhook_request.update({"signature": webhook_response_hash})

        views.create_request_log(
            db,
            request_data.get('requestId'),
            '',
            webhook_request,
            'response'
        )
        return webhook_request
    except Exception as e:
        logger.error(f"Error in sync_finance {e}")
        logger.error(f"Error in sync_finance {traceback.format_exc()}")
        return {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(500)
        }


@router.post("/syncDisbursement/{merchant_key}", description=TagsMetadata.sync_disbursement.get('description'))
def sync_disbursement(merchant_key: str, request: AsyncDisburseSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)

    try:
        api_request_log_res = views.create_request_log(db, request_data.get('requestId'), request_data, '', 'request', 'syncDisbursement', merchant_key)
        if api_request_log_res.get("code") != 200:
            return {
                "requestId": request_data.get('requestId'),
                **api_request_log_res
            }

        validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
        if validate_signature_response.get('code') != 200:
            # create response log
            views.create_request_log(db, request.requestId, '', validate_signature_response, 'response')
            return validate_signature_response

        # validate ledger date
        val_date_resp = utils.validate_ledger_date(request_data)
        if val_date_resp.get('code') != 200:
            return val_date_resp

        return_response = AsyncDisbursingFund.disbursing_invoice(db, request_data, merchant_key)
        resp_hash = utils.create_response_hash(db, return_response, merchant_key)
        return_response.update({"signature": resp_hash})

        views.create_request_log(
            db,
            request_data.get('requestId'),
            '',
            return_response,
            'response'
        )
        return return_response
    except Exception as e:
        logger.error(f"Error in sync_disbursement {e}")
        logger.error(f"Error in sync_disbursement {traceback.format_exc()}")
        return {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(500)
        }


@router.post("/syncRepayment/{merchant_key}", description=TagsMetadata.sync_repayment.get('description'))
def sync_repayment(merchant_key: str, request: AsyncRepaymentSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)

    try:
        api_request_log_res = views.create_request_log(db, request_data.get('requestId'), request_data, '', 'request', 'syncRepayment', merchant_key)
        if api_request_log_res.get("code") != 200:
            return {
                "requestId": request_data.get('requestId'),
                **api_request_log_res
            }

        validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
        if validate_signature_response.get('code') != 200:
            # create response log
            views.create_request_log(db, request.requestId, '', validate_signature_response, 'response')
            return validate_signature_response

        # validate ledger date
        val_date_resp = utils.validate_ledger_date(request_data)
        if val_date_resp.get('code') != 200:
            return val_date_resp

        return_response = AsyncRepayment.repayment_invoice(db, request_data, merchant_key)
        resp_hash = utils.create_response_hash(db, return_response, merchant_key)
        return_response.update({"signature": resp_hash})

        views.create_request_log(
            db,
            request_data.get('requestId'),
            '',
            return_response,
            'response'
        )
        return return_response
    except Exception as e:
        logger.error(f"Error in sync_repayment {e}")
        logger.error(f"Error in sync_repayment {traceback.format_exc()}")
        return {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(500)
        }


@router.post("/encryptPayload/{merchant_key}", description=TagsMetadata.sync_financing.get('description'))
async def encrypt_payload(merchant_key: str, request: Request, db: Session = Depends(get_db)):
    from utils import get_financial_year, check_invoice_date, create_post_processing, validate_signature, decrypt_aes_256, encrypt_aes_256, validate_hub_signature, generate_aes_key
    request_data = await request.json()
    logger.info(f"Request body data {request_data}")

    try:
        request_data = request_data
        merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
        if not merchant_details:
            return {
                **ErrorCodes.get_error_response(1002)
            }

        key = generate_aes_key(str(merchant_details.merchant_secret))
        enc_pay_load = encrypt_aes_256(key, request_data)
        return {
            'encryptedPayload': enc_pay_load
        }
    except Exception as e:
        logger.error(f"Error in sync_finance {e}")
        logger.error(f"Error in sync_finance {traceback.format_exc()}")
        return {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(500)
        }


@router.post("/decryptPayload/{merchant_key}", description=TagsMetadata.sync_financing.get('description'))
async def payload_decrypt(merchant_key: str, request: Request, db: Session = Depends(get_db)):
    from utils import get_financial_year, check_invoice_date, create_post_processing, validate_signature, decrypt_aes_256, encrypt_aes_256, validate_hub_signature, generate_aes_key
    request_data = await request.json()
    logger.info(f"Request body data {request_data}")

    try:
        request_data = request_data
        merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
        if not merchant_details:
            return {
                **ErrorCodes.get_error_response(1002)
            }

        key = generate_aes_key(str(merchant_details.merchant_secret))
        enc_pay_load = decrypt_aes_256(key, request_data.get('encryptedPayload'))
        return {
            'decryptedPayload': enc_pay_load
        }
    except Exception as e:
        logger.error(f"Error in sync_finance {e}")
        logger.error(f"Error in sync_finance {traceback.format_exc()}")
        return {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(500)
        }


@router.post("/postAsyncHubResp/{merchant_key}", description=TagsMetadata.sync_financing.get('description'))
async def payload_decrypt(merchant_key: str, request: Request, db: Session = Depends(get_db)):

    request_data = await request.json()
    logger.info(f"Request body data {request_data}")
    try:
        request_data = request_data
        from config import post_async_hub_resp_to_rbi
        merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
        if not merchant_details:
            return {
                **ErrorCodes.get_error_response(1002)
            }
        resp = post_async_hub_resp_to_rbi()

        return resp
    except Exception as e:
        logger.error(f"Error in sync_finance {e}")
        logger.error(f"Error in sync_finance {traceback.format_exc()}")
        return {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(500)
        }


# from models import FinancingApiViewModel
# from typing import List, Union
# @router.get("/custom-view")
# def get_custom_view(db: Session = Depends(get_db)):
#     query = "SELECT * FROM financing_api_view;"
#     result = db.execute(query)
#     return [FinancingApiViewModel(**row) for row in result.fetchall()]