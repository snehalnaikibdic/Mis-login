import datetime
import json
import logging
import traceback

import redis
from typing import Annotated
from fastapi import APIRouter, Depends, Request
from fastapi.encoders import jsonable_encoder
from io import BytesIO
import zipfile

import models
import utils
import views
from aes_encryption_decryption import AESCipher
from cygnet_api import CygnetApi
from database import SessionLocal
from sqlalchemy.orm import Session
from sqlalchemy import desc
from decouple import config as dconfig

from errors import ErrorCodes
from finance_view import AsyncFinancing, AsyncDisbursingFund, AsyncRepayment, webhook_finance_data
from gspi_api import verify_ewb, get_status, download
from models import MerchantDetails, EntityCombination, Invoice, VayanaTaskHistory
from registration_view import AsyncRegistration, AsyncInvoiceRegistrationCode
from routers.auth import get_current_merchant_active_user, User
from schema import AsyncFinanceSchema, AsyncDisburseSchema, AsyncRepaymentSchema, BulkAsyncFinanceSchema, \
    InvoiceBulkRequestSchema, InvoiceBulkRequestWithoutCodeFinSchema, \
    InvoiceBulkRequestWithoutCodeFinDisbursementSchema, InvoiceWithCodeBulkRequestSchema, \
    InvoiceWithCodeFinanceBulkRequestSchema, InvoiceRequestWithCodeFinanceDisbursementGroupSchema, \
    BulkAsyncDisbursementSchema, BulkAsyncRepaymentSchema, BulkAsyncDisbursementRepaymentSchema
from utils import get_financial_year, check_invoice_date, create_post_processing, validate_signature, SpecialCharRemove
from config import webhook_task
from database import get_db
from routers.registration import validate_seller_buyer_code

logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)

class TagsMetadata:
    async_bulk_financing_info = {
        "name": "asyncBulkFinancing",
        "description": "**Aim** : Registered Invoices are eligible for financing and this api will update the status of ledger and invoices to funded.\n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- Ledger data for all the invoices in the ledger should be provided in the financing api\n"
                       "- Invoice amount should be the same as that provided in the registration api Adjustment amount should be less than or equal to invoice amount \n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n **Function** : create_bulk_request_log, validate_signature, bulk_async_invoice_financing, \n"
                       "fin_req_validation, finance_invoice,webhook_finance_data, bulk_post_webhook_data.",
        "summary": "Summary of async bulk financing"
    }
    async_bulk_disbursement_info = {
        "name": "asyncBulkDisbursement",
        "description": "**Aim** : Funded Invoices will be disbursed and Ledger and Invoice status marked to partial/full_disbursed.\n"
                       "\n**Validation** : \n"
                       "- Disbursed amount should be less than or equal to finance amount\n"
                       "- Disbursed date should be later than financed date & invoice date \n"
                       "- Due date should be later than disbursed date \n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n **Function** : create_bulk_request_log, validate_signature, disbursement_req_validation, async_invoice_disbursing, \n"
                       "disbursing_invoice, disburse_ledger, check_invoice, bulk_post_webhook_data",
        "summary": "async bulk disbursement summary ",
    }
    async_bulk_repayment_info = {
        "name": "asyncBulkRepayment",
        "description": "**Aim** : On Recieving payment of Disbursed Invoices, this api will mark Ledger and Invoice status to repaid/partial_repaid.\n"
                       "\n**Validation** : \n"
                       "- requestId format validation\n"
                       "- Borrower Category format validation\n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "- Date format validation\n"
                       "\n**Table** :BulkAPIRequestLog, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n**Function** : create_bulk_request_log, validate_signature, async_invoice_repayment, \n"
                       " repayment_invoice, repayment_ledger, repayment_ledger, check_invoice.",
        "summary": "async bulk repayment summary "
    }
    async_bulk_invoice_registration_withcode_info = {
        "name": "Async Bulk Invoice Registration With Entity Code",
        "description": "**Aim** : This API for invoice registration require to be with buyer and seller entities are "
                       "already pre registered. IDP will request to register the invoice/ group of invoices against "
                       "themselves, and a ledger ID created.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, MerchantDetails, "
                       "EntityIdentifierLine, EntityCombination, Entity, InvoiceEncryptedData.\n"
                       "\n **Function** : create_bulk_request_log, validate_signature, "
                       "create_invoice_post \n"
                       "check_entity_code_gst_validation, check_create_derived_pan_seller, "
                       "check_create_derived_pan_buyer, "
                       " create_invoice, create_ledger_hash_and_validate, create_ledger, "
                       "bulk_post_webhook_data.",
        "summary": "Summary of asyncRegistrationWithCode"
    } create_ledger_hash_and_validate
    async_bulk_invoice_registration_withoutcode_info = {
        "name": "Async Bulk Invoice Registration Without Entity Code",
        "description": "**Aim** : This API for invoice registration does not require entity code. IDP will request to"
                       " register the invoice/ group of invoices against themselves, and a ledger ID created via a "
                       "Webhook-based response will be returned.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, MerchantDetails, "
                       "EntityIdentifierLine, Entity, InvoiceEncryptedData.\n"
                       "\n **Function** : create_bulk_request_log, validate_signature, "
                       "create_invoice_post \n"
                       "check_entity_code_gst_validation, update_existing_seller_identifier,  "
                       "update_existing_buyer_identifier, "
                       "add_buyer_identifier_lines, create_buyer_identifier_conditionaly, "
                       "add_seller_identifier_lines, create_seller_identifier_conditionaly, create_invoice, "
                       "create_ledger, "
                       "check_buyer_identifier_data, create_ledger_hash_and_validate, validate_ledger_hash,"
                       " check_entity_code_gst_validation, bulk_post_webhook_data.",
        "summary": "Summary of asyncValidationServiceWithoutCode"
    }
    async_validation_service_without_code_info = {
        "name": "async validation service without entity code",
        "description": "**Aim** : Before establishing the invoice registration service without the entity code called, "
                       "this API will be made available to verify whether the format of all requested data is correct "
                       "or not in the validation service for invoices without entity code format via webhook.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- Date format validation\n"
                       "- Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, EntityCombination\n"
                       "\n **Function** : create_bulk_request_log, validate_signature, validation_service_without_code, \n"
                       "check_entity_code_gst_validation, bulk_post_webhook_data",
        "summary": "Summary of asyncValidationServiceWithoutCode"
    }
    async_validation_service_with_code_info = {
        "name": "async validation service with entity code",
        "description": "**Aim** : Before establishing the invoice registration service with the entity code called,"
                       "this API will be made available to verify whether the format of all requested data is correct"
                       "or not in the validation service with the entity code format via webhook.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, EntityCombination\n"
                       "\n **Function** :validate_signature, create_bulk_request_log, \n"
                       "validation_service_with_code, check_entity_code_gst_validation, webhook_task",
        "summary": "Summary of asyncValidationServiceWithCode"
    }

    async_bulk_disbursement_repayment_info = {
        "name": "asyncBulkDisbursementRepaymant",
        "description": "**Aim** : Multiple funded invoice will be disbursed and Ledger and Invoice status marked to"
                       " partial/full_disbursed, after that On Recieving payment of Disbursed Invoices, this api will mark"
                       " Ledger and Invoice status to repaid/partial_repaid.\n"
                       "\n**Validation** : \n"
                       "- Disbursed amount should be less than or equal to finance amount\n"
                       "- Disbursed date should be later than financed date & invoice date \n"
                       "- requestId format validation\n"
                       "- Borrower Category format validation\n"
                       "- Ledger should be existing \n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "- Date format validation\n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, lenderDetails, lenderInvoiceAssociation\n"
                       "\n **Function** :create_bulk_request_log, validate_signature, disbursement_req_validation, async_invoice_disbursing, disbursing_invoice, disburse_ledger, check_invoice, bulk_post_webhook_data"
    }

    async_bulk_invoice_registration_withoutcode_financing_info = {
        "name": "Async Bulk Invoice Registration Without Entity Code and financing",
        "description": "**Aim** : This API for invoice registration require to be with buyer and seller "
                       "entities are already pre registered. IDP will request to register the invoice/ group of "
                       "invoices against themselves, and a ledger ID created after that financing update the status of "
                       "ledger and invoice to funded via a Webhook-based response will be returned.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "- Ledger data for all the invoices in the ledger should be provided in the financing api\n"
                       "- Invoice amount should be the same as that provided in the registration api Adjustment amount should be less than or equal to invoice amount \n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"                       
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, MerchantDetails, "
                       "EntityIdentifierLine, Entity, InvoiceEncryptedData,lenderDetails, lenderInvoiceAssociation \n"
                       "\n **Function** : create_bulk_request_log, validate_signature, "
                       "create_bulk_invoice_post \n"
                       "check_entity_code_gst_validation, update_existing_seller_identifier,  "
                       "update_existing_buyer_identifier, "
                       "add_buyer_identifier_lines, create_buyer_identifier_conditionaly, "
                       "add_seller_identifier_lines, create_seller_identifier_conditionaly, create_invoice, "
                       "create_ledger, "
                       "check_buyer_identifier_data, create_ledger_hash_and_validate, validate_ledger_hash,"
                       " check_entity_code_gst_validation, async_bulk_invoice_reg_financing, bulk_post_webhook_data.",
        "summary": "Summary of Async Bulk Invoice Registration Without Entity Code and financing"
    }

    async_bulk_invoice_registration_withoutcode_financing_disbursement_info = {
        "name": "Async Bulk Invoice Registration Without Entity Code, financing and disbursement",
        "description": "**Aim** : This API for invoice registration require to be with buyer and seller "
                       "entities are already pre registered. IDP will request to register the invoice/ group of "
                       "invoices against themselves, and a ledger ID created after that financing update the status of "
                       "ledger and invoice to funded, funded invoices will be disbursed, ledger and invoice status marked to partial/full_disbursed via a Webhook-based response.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "- Ledger data for all the invoices in the ledger should be provided in the financing api\n"
                       "- Invoice amount should be the same as that provided in the registration api Adjustment amount should be less than or equal to invoice amount \n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "- Disbursed amount should be less than or equal to finance amount\n"
                       "- Disbursed date should be later than financed date & invoice date \n"
                       "- Due date should be later than disbursed date \n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, MerchantDetails, "
                       "EntityIdentifierLine, Entity, InvoiceEncryptedData,lenderDetails, lenderInvoiceAssociation \n"
                       "\n **Function** : create_bulk_request_log, validate_signature, "
                       "create_bulk_invoice_post \n"
                       "check_entity_code_gst_validation, update_existing_seller_identifier,  "
                       "update_existing_buyer_identifier, "
                       "add_buyer_identifier_lines, create_buyer_identifier_conditionaly, "
                       "add_seller_identifier_lines, create_seller_identifier_conditionaly, create_invoice, "
                       "create_ledger, "
                       "check_buyer_identifier_data, create_ledger_hash_and_validate, validate_ledger_hash,"
                       " check_entity_code_gst_validation, async_bulk_invoice_reg_financing,async_invoice_disbursing,disbursing_invoice,"
                       "disburse_ledger, bulk_post_webhook_data.",
        "summary": "Summary of Async Bulk Invoice Registration Without Entity Code and financing"
    }

    async_bulk_invoice_registration_withcode_financing_info = {
        "name": "new consent user",
        "description": "**Aim** : This API for invoice registration require to be with buyer and seller entities are "
                       "already pre registered. IDP will request to register the invoice/ group of invoices against "
                       "themselves, and a ledger ID created and Registered Invoices are eligible for financing and this api will update the status of ledger and invoices to funded.\n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "- Ledger data for all the invoices in the ledger should be provided in the financing api\n"
                       "- Invoice amount should be the same as that provided in the registration api Adjustment amount should be less than or equal to invoice amount \n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "\n **Table** :  Invoice, Ledger, MerchantDetails, "
                       "EntityIdentifierLine, EntityCombination, Entity, InvoiceEncryptedData, lenderDetails, "
                       "lenderInvoiceAssociation\n"
                       "\n **Function** : create_bulk_request_log, validate_signature, "
                       "create_invoice_post \n"
                       "check_entity_code_gst_validation, check_create_derived_pan_seller, "
                       "check_create_derived_pan_buyer, "
                       " create_invoice, create_ledger_hash_and_validate, create_ledger, finance_invoice, fin_req_validation"
                       "bulk_post_webhook_data.",
        "summary": "Summary of async_bulk_invoice_registration_withcode_financing"
    }
    async_bulk_invoice_registration_withcode_financing_disbursement_info = {
        "name": "new consent user",
        "description": "**Aim** : This API for invoice registration require to be with buyer and seller entities are "
                       "already pre registered. IDP will request to register the invoice/ group of invoices against "
                       "themselves, and a ledger ID created and Registered Invoices are eligible for financing and this api will update the status of ledger and invoices to funded. all founded invoice will be "
                       "disbursed and Ledger and Invoice status marked to partial/full_disbursed.\n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "- Ledger data for all the invoices in the ledger should be provided in the financing api\n"
                       "- Invoice amount should be the same as that provided in the registration api Adjustment amount should be less than or equal to invoice amount \n"
                       "- Ledger should be existing\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "- Disbursed amount should be less than or equal to finance amount\n"
                       "- Disbursed date should be later than financed date & invoice date \n"
                       "- Due date should be later than disbursed date \n"
                       "\n **Table** : BulkAPIRequestLog, Invoice, Ledger, MerchantDetails, "
                       "EntityIdentifierLine, EntityCombination, Entity, InvoiceEncryptedData, lenderDetails, "
                       "lenderInvoiceAssociation\n"
                       "\n **Function** : create_bulk_request_log, validate_signature, "
                       "create_invoice_post \n"
                       "check_entity_code_gst_validation, check_create_derived_pan_seller, "
                       "check_create_derived_pan_buyer, "
                       " create_invoice, create_ledger_hash_and_validate, create_ledger, finance_invoice, fin_req_validation",
                       "disbursement_req_validation, async_invoice_disbursing, disbursing_invoice, disburse_ledger,"
                       "bulk_post_webhook_data.\n"
        "summary": "Summary of async_bulk_invoice_registration_withcode_financing_disbursement"
    }


@router.post("/async-bulk-financing/{merchant_key}", description=TagsMetadata.async_bulk_financing_info.get('description'))
def bulk_async_finance(merchant_key: str, request: BulkAsyncFinanceSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    request_id = request_data.get('requestId')
    return_response = {
        'requestId': request_id,
        **ErrorCodes.get_error_response(1028)
    }
    bulk_api_req_log_res = utils.create_bulk_request_log(db, request_id, request_data, '', 'request',
                                                             'async_bulk_financing', merchant_key)
    if bulk_api_req_log_res.get("code") != 200:
        return_response.update(bulk_api_req_log_res)
        response_hash = utils.create_response_hash(db, return_response, merchant_key)
        return_response.update({"signature": response_hash})
        return return_response

    validate_signature_response = validate_signature(db, request_data, merchant_key)
    if validate_signature_response.get('code') != 200:
        return_response.update(validate_signature_response)
        response_hash = utils.create_response_hash(db, return_response, merchant_key)
        return_response.update({"signature": response_hash})
        utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
        return return_response

    grouping_ids = set()
    for new_request_data in request_data.get('groupData'):
        # fin_req_val_resp = AsyncFinancing.fin_req_validation(new_request_data)
        # if fin_req_val_resp.get('code') != 200:
        #     return_response.update(fin_req_val_resp)
        #     response_hash = utils.create_response_hash(db, return_response, merchant_key)
        #     return_response.update({"signature": response_hash})
        #     utils.create_bulk_request_log(db, request_id,'', return_response, 'response')
        #     return return_response

        # make sure groupingId is unique
        current_grouping_id = new_request_data["groupingId"]
        if current_grouping_id in grouping_ids:
            return_response.update(ErrorCodes.get_error_response(1099))
            response_hash = utils.create_response_hash(db, return_response, merchant_key)
            return_response.update({"signature": response_hash})
            utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
            return return_response
        else:
            grouping_ids.add(current_grouping_id)

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(db, request_id, '', return_response, 'response')

    webhook_task.delay(
        request_data,
        merchant_key,
        'async_bulk_financing'
    )

    return return_response


@router.post("/async-bulk-disbursement/{merchant_key}", description=TagsMetadata.async_bulk_disbursement_info.get('description'))
def bulk_async_disbursement(merchant_key: str, request: BulkAsyncDisbursementSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    request_id = request_data.get('requestId')
    return_response = {
        'requestId': request_id,
        **ErrorCodes.get_error_response(1029)
    }
    bulk_api_req_log_res = utils.create_bulk_request_log(db, request_id, request_data, '', 'request',
                                                             'async_bulk_disbursement', merchant_key)
    if bulk_api_req_log_res.get("code") != 200:
        return_response.update(bulk_api_req_log_res)
        response_hash = utils.create_response_hash(db, return_response, merchant_key)
        return_response.update({"signature": response_hash})
        return return_response

    validate_signature_response = validate_signature(db, request_data, merchant_key)
    if validate_signature_response.get('code') != 200:
        return_response.update(validate_signature_response)
        response_hash = utils.create_response_hash(db, return_response, merchant_key)
        return_response.update({"signature": response_hash})
        utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
        return return_response

    grouping_ids = set()
    for new_request_data in request_data.get('groupData'):
        dis_req_val_resp = disbursement_req_validation(
            request_data.get('groupData'),
            request_data.get('requestId')
        )
        if dis_req_val_resp.get('code') != 200:
            return_response.update(dis_req_val_resp)
            response_hash = utils.create_response_hash(db, return_response, merchant_key)
            return_response.update({"signature": response_hash})
            utils.create_bulk_request_log(db, request_id,'', return_response, 'response')
            return return_response

        # make sure groupingId is unique
        current_grouping_id = new_request_data["groupingId"]
        if current_grouping_id in grouping_ids:
            return_response.update(ErrorCodes.get_error_response(1099))
            response_hash = utils.create_response_hash(db, return_response, merchant_key)
            return_response.update({"signature": response_hash})
            utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
            return return_response
        else:
            grouping_ids.add(current_grouping_id)

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(db, request_id, '', return_response, 'response')

    webhook_task.delay(
        request_data,
        merchant_key,
        'async_bulk_disbursement'
    )

    return return_response


@router.post("/async-bulk-repayment/{merchant_key}", description=TagsMetadata.async_bulk_repayment_info.get('description'))
def bulk_async_repayment(merchant_key: str, request: BulkAsyncRepaymentSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    request_id = request_data.get('requestId')
    return_response = {
        'requestId': request_id,
        **ErrorCodes.get_error_response(1033)
    }
    bulk_api_req_log_res = utils.create_bulk_request_log(db, request_id, request_data, '', 'request',
                                                             'async_bulk_repayment', merchant_key)
    if bulk_api_req_log_res.get("code") != 200:
        return_response.update(bulk_api_req_log_res)
        response_hash = utils.create_response_hash(db, return_response, merchant_key)
        return_response.update({"signature": response_hash})
        return return_response

    validate_signature_response = validate_signature(db, request_data, merchant_key)
    if validate_signature_response.get('code') != 200:
        return_response.update(validate_signature_response)
        response_hash = utils.create_response_hash(db, return_response, merchant_key)
        return_response.update({"signature": response_hash})
        utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
        return return_response

    grouping_ids = set()
    for new_request_data in request_data.get('groupData'):
        rep_req_val_resp = repayment_req_validation(
            request_data.get('groupData'),
            request_data.get('requestId')
        )
        if rep_req_val_resp.get('code') != 200:
            return_response.update(rep_req_val_resp)
            response_hash = utils.create_response_hash(db, return_response, merchant_key)
            return_response.update({"signature": response_hash})
            utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
            return return_response

        # make sure groupingId is unique
        current_grouping_id = new_request_data["groupingId"]
        if current_grouping_id in grouping_ids:
            return_response.update(ErrorCodes.get_error_response(1099))
            response_hash = utils.create_response_hash(db, return_response, merchant_key)
            return_response.update({"signature": response_hash})
            utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
            return return_response
        else:
            grouping_ids.add(current_grouping_id)

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(db, request_id, '', return_response, 'response')

    # webhook_task(
    webhook_task.delay(
        request_data,
        merchant_key,
        'async_bulk_repayment'
    )

    return return_response


@router.post("/async-bulk-disbursement-repayment/{merchant_key}", description=TagsMetadata.async_bulk_disbursement_repayment_info.get('description'))
def async_bulk_disbursement_repayment(
        merchant_key: str, request: BulkAsyncDisbursementRepaymentSchema, db: Session = Depends(get_db)
):
    request_data = jsonable_encoder(request)
    request_id = request_data.get('requestId')
    logger.info(f" ************ async-bulk-disbursement-repayment : Request data : {request_data} **************")
    return_response = {
        "requestId": request_id,
        **ErrorCodes.get_error_response(1029)
    }

    bulk_api_request_log_res = utils.create_bulk_request_log(
        db,
        request_id,
        request_data, '',
        'request',
        'async-bulk-disbursement-repayment', merchant_key
    )
    if bulk_api_request_log_res.get("code") == 200:
        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            disbursement_req_val_resp = disbursement_req_validation(
                request_data.get('groupData'),
                request_data.get('requestId')
            )
            if disbursement_req_val_resp.get('code') == 200:
                repayment_req_val_resp = repayment_req_validation(
                    request_data.get('groupData'),
                    request_data.get('requestId')
                )
                if repayment_req_val_resp.get('code') == 200:
                    # webhook_task.delay(
                    webhook_task(
                        request_data,
                        merchant_key,
                        'async_bulk_disbursement_repayment'
                    )
                else:
                    return_response = repayment_req_val_resp
            else:
                return_response = disbursement_req_val_resp

        else:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId": request_id,
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
    return return_response


@router.post("/async-bulk-registration-without-code/{merchant_key}", description=TagsMetadata.async_bulk_invoice_registration_withoutcode_info.get('description'))
def async_bulk_registration_without_code(merchant_key: str, request: InvoiceBulkRequestSchema,
                                         db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    bulk_api_request_log_res = utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        request_data,
        '',
        'request',
        'async-bulk-registration-without-code',
        merchant_key
    )

    return_response = {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1023)}
    if bulk_api_request_log_res.get("code") == 200:
        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            validate_bulk_response = validate_bulk_request(request_data.get('groupData'), db, merchant_key)
            if validate_bulk_response.get('code') == 200:
                webhook_task.delay(request_data, merchant_key, 'async_bulk_invoice_registration')
            else:
                return_response = {
                    "requestId": request_data.get('requestId'),
                    **validate_bulk_response
                }
        else:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId": request_data.get('requestId'),
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/async-bulk-registration-without-code-finance/{merchant_key}", description=TagsMetadata.async_bulk_invoice_registration_withoutcode_financing_info.get('description'))
def async_bulk_registration_without_code_finance(merchant_key: str, request: InvoiceBulkRequestWithoutCodeFinSchema,
                                         db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    bulk_api_request_log_res = utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        request_data,
        '',
        'request',
        'async-bulk-registration-without-code-finance',
        merchant_key
    )

    return_response = {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1023)}
    if bulk_api_request_log_res.get("code") == 200:
        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            validate_bulk_response = validate_bulk_request(request_data.get('groupData'), db, merchant_key)
            if validate_bulk_response.get('code') == 200:
                fin_req_val_resp = fin_req_validation(
                    request_data.get('groupData'),
                    request_data.get('requestId')
                )
                if fin_req_val_resp.get('code') == 200:
                    webhook_task.delay(request_data, merchant_key, 'async_bulk_invoice_registration_without_code_fin')
                    # AsyncRegistration.create_bulk_invoice_post(
                    #     db,
                    #     request_data,
                    #     merchant_key,
                    #     'finance'
                    # )
                else:
                    return_response = fin_req_val_resp
            else:
                return_response = {
                    "requestId": request_data.get('requestId'),
                    **validate_bulk_response
                }
        else:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId": request_data.get('requestId'),
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/async-bulk-registration-without-code-finance-disbursement/{merchant_key}", description=TagsMetadata.async_bulk_invoice_registration_withoutcode_financing_disbursement_info.get('description'))
def async_bulk_registration_without_code_finance_disbursement(
        merchant_key: str,
        request: InvoiceBulkRequestWithoutCodeFinDisbursementSchema,
        db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    bulk_api_request_log_res = utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        request_data,
        '',
        'request',
        'async-bulk-registration-without-code-finance-disbursement',
        merchant_key
    )

    return_response = {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1023)}
    if bulk_api_request_log_res.get("code") == 200:
        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            validate_bulk_response = validate_bulk_request(request_data.get('groupData'), db, merchant_key)
            if validate_bulk_response.get('code') == 200:
                fin_req_val_resp = fin_req_validation(
                    request_data.get('groupData'),
                    request_data.get('requestId')
                )
                if fin_req_val_resp.get('code') == 200:
                    disbursement_resp = disbursement_req_validation(
                        request_data.get('groupData'),
                        request_data.get('requestId')
                    )
                    if disbursement_resp.get('code') == 200:
                        webhook_task.delay(
                            request_data,
                            merchant_key,
                            'async_bulk_invoice_registration_without_code_fin_disbursement'
                        )
                        # AsyncRegistration.create_bulk_invoice_post(
                        #     db,
                        #     request_data,
                        #     merchant_key,
                        #     'finance_disbursement'
                        # )
                    else:
                        return_response = disbursement_resp
                else:
                    return_response = fin_req_val_resp
            else:
                return_response = {
                    "requestId": request_data.get('requestId'),
                    **validate_bulk_response
                }
        else:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId": request_data.get('requestId'),
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/async-bulk-registration-with-code/{merchant_key}", description=TagsMetadata.async_bulk_invoice_registration_withcode_info.get('description'))
def async_bulk_registration_with_code(
        merchant_key: str,
        request: InvoiceWithCodeBulkRequestSchema,
        db: Session = Depends(get_db)
):
    request_data = jsonable_encoder(request)
    request_id = request_data.get('requestId')
    return_response = {
        "requestId": request_id,
        **ErrorCodes.get_error_response(1023)
    }
    bulk_api_request_log_res = utils.create_bulk_request_log(db, request_id, request_data, '', 'request',
                                                             'async-bulk-registration-with-code', merchant_key
                                                             )
    if bulk_api_request_log_res.get("code") == 200:
        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            validate_entity_code_response = validate_bulk_request_for_reg_with_code(
                request_data.get('groupData'),
                validate_signature_response.get('merchant_details'),
                db,
                request_id
            )
            if validate_entity_code_response.get('code') == 200:
                # webhook_task.delay(request_data, merchant_key, 'async_bulk_invoice_registration_with_code')
                AsyncInvoiceRegistrationCode.create_bulk_invoice_post(
                    db,
                    request_data,
                    merchant_key
                )
            else:
                return_response = {
                    "requestId": request_id,
                    **validate_entity_code_response
                }
        else:
            return_response = {
                "requestId": request_id,
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId": request_id,
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
    return return_response


@router.post("/async-bulk-registration-with-code-finance/{merchant_key}", description=TagsMetadata.async_bulk_invoice_registration_withcode_financing_info.get('description'))
def async_bulk_registration_with_code_finance(merchant_key: str, request: InvoiceWithCodeFinanceBulkRequestSchema,
                                         db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    request_id = request_data.get('requestId')
    logger.info(f" ************ getting request data with code finance {request_data} **************")
    return_response = {
        "requestId": request_id,
        **ErrorCodes.get_error_response(1023)
    }
    bulk_api_request_log_res = utils.create_bulk_request_log(db, request_id, request_data, '', 'request',
                                                             'async-bulk-registration-with-code-finance', merchant_key)
    if bulk_api_request_log_res.get("code") == 200:
        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            # validate_bulk_response = validate_bulk_request(request_data.get('groupData'), db, merchant_key)
            # if validate_signature_response.get('code') == 200:
            validate_entity_code_response = validate_bulk_request_for_reg_with_code(
                request_data.get('groupData'),
                validate_signature_response.get('merchant_details'),
                db,
                request_id
            )
            if validate_entity_code_response.get('code') == 200:
                fin_req_val_resp = fin_req_validation(
                    request_data.get('groupData'),
                    request_data.get('requestId')
                )
                if fin_req_val_resp.get('code') == 200:
                    webhook_task.delay(request_data, merchant_key, 'async_bulk_invoice_registration_with_code_fin')
                    # AsyncInvoiceRegistrationCode.create_bulk_invoice_post(
                    #     db,
                    #     request_data,
                    #     merchant_key,
                    #     'finance'
                    # )
                else:
                    return_response = fin_req_val_resp
            else:
                return_response = validate_entity_code_response
            # else:
            #     return_response = {
            #         "requestId": request_id,
            #         **validate_bulk_response
            #     }
        else:
            return_response = {
                "requestId": request_id,
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId": request_id,
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
    return return_response


@router.post("/async-bulk-registration-with-code-finance-disbursement/{merchant_key}", description=TagsMetadata.async_bulk_invoice_registration_withcode_financing_disbursement_info.get('description'))
def async_bulk_registration_with_code_finance_disbursement(
        merchant_key: str, request: InvoiceRequestWithCodeFinanceDisbursementGroupSchema, db: Session = Depends(get_db)
):
    request_data = jsonable_encoder(request)
    request_id = request_data.get('requestId')
    logger.info(f" ************ async-bulk-registration-with-code-finance-disbursement : Request data : {request_data} **************")
    return_response = {
        "requestId": request_id,
        **ErrorCodes.get_error_response(1023)
    }
    bulk_api_request_log_res = utils.create_bulk_request_log(
        db,
        request_id,
        request_data, '',
        'request',
        'async-bulk-registration-with-code-finance-disbursement', merchant_key
    )
    if bulk_api_request_log_res.get("code") == 200:
        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            validate_bulk_response = validate_bulk_request_for_reg_with_code(
                request_data.get('groupData'),
                validate_signature_response.get('merchant_details'),
                db,
                request_id
            )
            if validate_bulk_response.get('code') == 200:
                fin_req_val_resp = fin_req_validation(
                    request_data.get('groupData'),
                    request_data.get('requestId')
                )
                if fin_req_val_resp.get('code') == 200:
                    disbursement_resp = disbursement_req_validation(
                        request_data.get('groupData'),
                        request_data.get('requestId')
                    )
                    if disbursement_resp.get('code') == 200:
                        # webhook_task.delay(
                        #     request_data,
                        #     merchant_key,
                        #     'async_bulk_registration_with_code_finance_disbursement'
                        # )
                        AsyncInvoiceRegistrationCode.create_bulk_invoice_post(
                            db,
                            request_data,
                            merchant_key,
                            'finance_disbursement'
                        )
                    else:
                        return_response = disbursement_resp
                else:
                    return_response = fin_req_val_resp
            else:
                return_response = {
                    "requestId": request_data.get('requestId'),
                    **validate_bulk_response
                }
        else:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId": request_id,
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(db, request_id, '', return_response, 'response')
    return return_response


@router.post("/test-gsp-api/{merchant_key}")
def test_gsp_api(merchant_key: str, request: dict, db: Session = Depends(get_db)):
    from gspi_api import get_token

    request_data = {
      "handle": "neha.joshi@ibdic.in",
      "password": "Vjti@2012",
      "handleType": "email",
      "tokenDurationInMins": 360
    }
    verify_request_data = {
        "payload": [
            {
                "ewbNumber": "671011764739",
                "docType": "INV",
                "docNo": "RAMA0003A1105",
                "docDate": "15/09/2023",
                "fromGstin": "24AAAPI3182M002",
                "toGstin": "19AAAPI3182M007",
                "totInvValue": 170580.8,
                "totalValue": 144560,
                "cgstValue": 0,
                "sgstValue": 0,
                "igstValue": 26020.8,
                "cessValue": 0.00,
                "otherValue": 0.00,
                "cessNonAdvolValue": 0.00
            },
            {
                "ewbNumber": "641011764743"
            },
            {
                "ewbNumber": "681011764745",
                "docType": "INV",
                "docNo": "RAMA0003A1107",
                "docDate": "15/09/2023",
                "fromGstin": "24AAAPI3182M002",
                "toGstin": "19AAAPI3182M007",
                "totInvValue": 170580.8,
                "totalValue": 144560,
                "cgstValue": 0,
                "sgstValue": 0,
                "igstValue": 26020.8
            }
        ],
        "meta": {
            "json": "Y"
        }
    }

    gsp_user_details = (
        db.query(
            models.GSPUserDetails
        ).filter(
            models.GSPUserDetails.gstin == request.get('gstin')
        ).first()
    )

    cache_org_id_key = f"org_id@test_24_001"
    cache_token_key = f"token@test_24_001"
    auth_token = r.get(cache_token_key)
    org_id = r.get(cache_org_id_key)
    if not auth_token:
        gsp_response, response_data = get_token(request_data, 'vanaya')
        if gsp_response.status_code != 200:
            return response_data

        auth_token = str(response_data.get('data').get('token'))
        org_id = str(response_data.get('data').get('associatedOrgs')[0].get('organisation').get('id'))
        r.set('token', auth_token)
        r.set('org_id', org_id)
        r.expire('token', 21600)
        r.expire('org_id', 21600)

    aes_encryption = AESCipher()
    aes_response = aes_encryption.vayana_encryption(gsp_user_details.password)
    encrypted_rek = aes_response.get('X-FLYNN-S-REK')
    encrypted_password = aes_response.get('X-FLYNN-S-DATA')
    verify_gsp_response, gsp_response_data = verify_ewb(
        verify_request_data,
        'vayana',
        org_id,
        auth_token,
        gsp_user_details,
        encrypted_rek,
        encrypted_password
    )
    if verify_gsp_response.status_code != 200:
        return gsp_response_data

    status_response, response_data = get_status(
        'vayana',
        org_id,
        auth_token,
        gsp_response_data.get('data').get('task-id'),
        gsp_user_details,
        encrypted_rek,
        encrypted_password
    )
    if status_response.status_code != 200:
        return response_data
    task_obj = VayanaTaskHistory(
        task_id=gsp_response_data.get('data').get('task-id'),
        task_id_status=response_data.get('data').get('status')
    )
    db.add(task_obj)
    db.commit()
    db.refresh(task_obj)

    if response_data.get('status') != "1":
        return response_data

    download_response = download(
        'vayana',
        org_id,
        auth_token,
        gsp_response_data.get('data').get('task-id')
    )
    # zip_file_path = '/home/akhilesh/Documents/download.zip'
    # extract_path = '/home/akhilesh/Documents/download'

    if download_response.status_code == 200:

        # Read the content of the zip file
        zip_data = BytesIO(download_response.content)

        # Open the zip file
        with zipfile.ZipFile(zip_data, 'r') as zip_ref:
            extracted_files = {name: zip_ref.read(name) for name in zip_ref.namelist()}

        # Now you can access the extracted files in memory
        for file_name, file_content in extracted_files.items():
            file_data = json.loads(file_content)
            if file_name == "result.json":
                for result_json in file_data.get('data'):
                    ewb_status = result_json.get('status')
                    ewb_no = result_json.get('additionalInfo').get('key').get('ewb-number')
                    if ewb_status != '1':
                        update_ewb_invoice(db, ewb_no, ewb_status)
            else:
                ewb_status = "1"
                ewb_no = str(file_data.get('ewbNo'))
                update_ewb_invoice(db, ewb_no, ewb_status, file_data)
    else:
        return download_response.json()

    # if status_response.status_code == 200:
    #     with open(zip_file_path, 'wb') as f:
    #         f.write(status_response.content)
    #
    #     with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    #         zip_ref.extractall(extract_path)
    #     with open("/home/akhilesh/Documents/result.json", "r") as file:
    #         data = json.load(file)
    #         for result_json in data.get('data'):
    #             ewb_status = result_json.get('status')
    #             ewb_no = result_json.get('additionalInfo').get('key').get('ewb-number')
    #             ewb_obj = (
    #                 db.query(
    #                     Invoice
    #                 ).filter(
    #                     Invoice.extra_data.contains({"ewb_no": ewb_no})
    #                 ).first()
    #             )
    #             if ewb_status != "1":
    #                 ewb_obj.gst_status = False
    #                 db.commit()
    #                 db.refresh(ewb_obj)
    #             else:
    #                 extract_file_path = f"/home/akhilesh/Documents/download/{ewb_no}.json"
    #                 with open(extract_file_path, "r") as extract_file:
    #                     extract_data = json.load(extract_file)
    #                     invoice_date = ewb_obj.invoice_date
    #                     ewb_obj_invoice_date = extract_data.get('docDate')
    #                     invoice_no = ewb_obj.invoice_date
    #                     ewb_invoice_no = extract_data.get('docNo')
    #                     invoice_formatted_date = invoice_date.strftime("%d/%m/%Y")
    #
    #                     if invoice_formatted_date != ewb_obj_invoice_date or invoice_no != ewb_invoice_no:
    #                         ewb_obj.gst_status = False
    #                         db.commit()
    #                         db.refresh(ewb_obj)


@router.post("/async-bulk-validation-service-without-code/{merchant_key}", description=TagsMetadata.async_validation_service_without_code_info.get('description'))
# "async_invoice_registration_without_code",
def async_bulk_validation_without_entity_code(merchant_key: str,
                      request:  InvoiceBulkRequestSchema,
                      db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    bulk_api_request_log_res = utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        request_data,
        '',
        'request',
        'async-bulk-validation-service-without-entity-code',
        merchant_key
    )

    return_response = {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1035)}
    if bulk_api_request_log_res.get("code") == 200:
        # validation invoice number, date , duplicate check and amount
        validate_bulk_inv_data_response = validate_bulk_invoice_data_formate(request_data.get('groupData'), db, merchant_key)
        if validate_bulk_inv_data_response.get('code') != 200:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_bulk_inv_data_response
            }

        validate_bulk_inv_date_response = validate_bulk_invoice_date(request_data)
        if validate_bulk_inv_date_response.get('code') != 200:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_bulk_inv_date_response
            }

        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            validate_bulk_response = validate_bulk_request(request_data.get('groupData'), db, merchant_key)
            if validate_bulk_response.get('code') == 200:
                webhook_task.delay(request_data, merchant_key, 'async_bulk_validation_service_without_code')
                # return_response = AsyncValidationServiceWithoutCode.validation_bulk_service_without_code(db, request_data, merchant_key)
            else:
                return_response = {
                    "requestId": request_data.get('requestId'),
                    **validate_bulk_response
                }
        else:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId":  request_data.get('requestId'),
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/async-bulk-validation-service-with-code/{merchant_key}", description=TagsMetadata.async_validation_service_with_code_info.get('description'))
def async_bulk_validation_with_entity_code(merchant_key: str,
                      request:  InvoiceWithCodeBulkRequestSchema,
                      db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    bulk_api_request_log_res = utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        request_data,
        '',
        'request',
        'async-bulk-validation-service-with-entity-code',
        merchant_key
    )

    return_response = {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1034)}
    if bulk_api_request_log_res.get("code") == 200:
        # validation invoice number, date , duplicate check and amount
        validate_bulk_inv_data_response = validate_bulk_invoice_data_formate(request_data.get('groupData'), db, merchant_key)
        if validate_bulk_inv_data_response.get('code') != 200:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_bulk_inv_data_response
            }

        validate_bulk_inv_date_response = validate_bulk_invoice_date(request_data)
        if validate_bulk_inv_date_response.get('code') != 200:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_bulk_inv_date_response
            }

        validate_signature_response = validate_signature(db, request_data, merchant_key)
        if validate_signature_response.get('code') == 200:
            validate_entity_code_response = validate_bulk_request_for_reg_with_code(
                request_data.get('groupData'),
                validate_signature_response.get("merchant_details"),
                db,
                request_data.get('requestId')
            )
            if validate_entity_code_response.get('code') == 200:
                webhook_task.delay(request_data, merchant_key, 'async_bulk_validation_service_with_code')
                # return_response = AsyncValidationServiceWithCode.bulk_validation_service_without_code(db, request_data, merchant_key)
            else:
                return_response = {
                    "requestId": request_data.get('requestId'),
                    **validate_entity_code_response
                }
        else:
            return_response = {
                "requestId": request_data.get('requestId'),
                **validate_signature_response
            }
    else:
        return_response = {
            "requestId":  request_data.get('requestId'),
            **bulk_api_request_log_res
        }

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_bulk_request_log(
        db,
        request_data.get('requestId'),
        '',
        return_response,
        'response'
    )
    return return_response


def validate_bulk_request(group_data, db, merchant_key):
    logger.info(f"getting request group packet {group_data}")
    grouping_ids = set()
    for single_request_data in group_data:
        seller_pan_response = utils.validate_seller_invoice_pan_gst_pan(single_request_data)
        buyer_pan_response = utils.validate_buyer_invoice_pan_gst_pan(single_request_data)
        if seller_pan_response:
            return {**ErrorCodes.get_error_response(1060)}
        if buyer_pan_response:
            return {**ErrorCodes.get_error_response(1059)}

        json_data = single_request_data

        if not json_data.get('sellerIdentifierData'):
            if json_data.get('sellerGst') == "":
                return {**ErrorCodes.get_error_response(1067)}
        if not json_data.get('buyerIdentifierData'):
            if json_data.get('buyerGst') == "":
                return {**ErrorCodes.get_error_response(1068)}

        current_grouping_id = single_request_data["groupingId"]

        if current_grouping_id in grouping_ids:
            print(f"Error: Duplicate groupingId found - {current_grouping_id}")
            return {**ErrorCodes.get_error_response(1099)}
        else:
            grouping_ids.add(current_grouping_id)

    print("Validation complete. No duplicate groupingId found.")
    return {**ErrorCodes.get_error_response(200)}


def fin_req_validation(request_group_data, request_id):

    for group_data in request_group_data:
        group_data.update({"requestId": request_id})
        fin_req_val_resp = AsyncFinancing.fin_req_validation(group_data)
        if fin_req_val_resp.get('code') != 200:
            return fin_req_val_resp

    return {**ErrorCodes.get_error_response(200)}


def disbursement_req_validation(request_group_data, request_id):

    for group_data in request_group_data:
        group_data.update({"requestId": request_id})
        val_date_resp = utils.validate_ledger_date(group_data)
        if val_date_resp.get('code') != 200:
            return val_date_resp

    return {**ErrorCodes.get_error_response(200)}


def repayment_req_validation(request_group_data, request_id):

    for group_data in request_group_data:
        group_data.update({"requestId": request_id})
        val_date_resp = utils.validate_ledger_date(group_data)
        if val_date_resp.get('code') != 200:
            return val_date_resp

    return {**ErrorCodes.get_error_response(200)}


def validate_bulk_request_for_reg_with_code(group_data, merchant_obj, db, request_id):
    logger.info(f"getting validate_bulk_request_for_reg_with_code {group_data}")
    grouping_ids = set()
    for request_data in group_data:
        seller_code = request_data.get('sellerCode')
        buyer_code = request_data.get('buyerCode')
        seller_gst = request_data.get('sellerGst')
        buyer_gst = request_data.get('buyerGst')

        if seller_code == buyer_code:
            return {"requestId": request_id, **ErrorCodes.get_error_response(1021)}

        if seller_gst and buyer_gst:
            if request_data.get('sellerGst') == request_data.get('buyerGst'):
                return {"requestId": request_id, **ErrorCodes.get_error_response(1022)}

        buyer_entity_combination_obj = db.query(
            EntityCombination
        ).filter(
            EntityCombination.merchant_id == merchant_obj.id,
            EntityCombination.entity_code == buyer_code,
            EntityCombination.is_active == True
        ).first()

        seller_entity_combination_obj = db.query(
            EntityCombination
        ).filter(
            EntityCombination.merchant_id == merchant_obj.id,
            EntityCombination.entity_code == seller_code,
            EntityCombination.is_active == True
        ).first()

        if not buyer_entity_combination_obj:
            return {"requestId": request_id, **ErrorCodes.get_error_response(1020)}

        if not seller_entity_combination_obj:
            return {"requestId": request_id, **ErrorCodes.get_error_response(1018)}

        current_grouping_id = request_data["groupingId"]

        if current_grouping_id in grouping_ids:
            print(f"Error: Duplicate groupingId found - {current_grouping_id}")
            return {**ErrorCodes.get_error_response(1099)}
        else:
            grouping_ids.add(current_grouping_id)

    return {**ErrorCodes.get_error_response(200)}


def update_ewb_invoice(db, ewb_no, ewb_status, file_data=None):
    ewb_obj = (
        db.query(
            Invoice
        ).filter(
            Invoice.extra_data.contains({"ewb_no": ewb_no})
        ).first()
    )
    if ewb_obj:
        if ewb_status != "1":
            ewb_obj.gst_status = False
            db.commit()
            db.refresh(ewb_obj)
        else:
            invoice_date = ewb_obj.invoice_date
            ewb_obj_invoice_date = file_data.get('docDate')
            invoice_no = ewb_obj.invoice_no
            ewb_invoice_no = file_data.get('docNo')
            invoice_formatted_date = invoice_date.strftime("%d/%m/%Y")

            ewb_obj.gst_status = True
            if invoice_formatted_date != ewb_obj_invoice_date or invoice_no != ewb_invoice_no:
                ewb_obj.gst_status = False
            db.commit()
            db.refresh(ewb_obj)


@router.post("/test-cygnet-api/{merchant_key}")
def test_cygnet_api(merchant_key: str, request: dict, db: Session = Depends(get_db)):
    from gspi_api import get_token
    try:
        request_data = {
            "data": {
                "f_name": "Sarthhi",
                "l_name": "Soshiio",
                "lgnm": "SD industriessi",
                "email": "sarth.doshi@cygnetinfotecc.com",
                "mo_num": "7878787878",
                "pan": "24AACPH8447G002",
                "ct": "1"
            }
        }
        consent_request_data = {
            "data": {
                "username": "CygnetGSP24",
                "password": "CygnetGSP24@Pass",
                "gstin": "24AACPH8447G002"
            }
        }

        get_ewaybill_bulk_request = {
            "data": {
                "ewbNo": "801008773758",
                "gstin": "24AACPH8447G002",
                "authtoken": "cTiNUykE4g9k26IbXnnWzQrbg",
                "sek": "+M3Stsxca3JT573hYws6gxCOFXPnYUr1AyA847wPjMU="
            }
        }

        ewb_obj = (
            db.query(models.Invoice)
            .filter(
                models.Invoice.extra_data.contains({"ewb_no": "801008773758"}),
            )
            .order_by(desc(
                models.Invoice.id
            ))
            .first()
        )
        dd = CygnetApi()
        # consent_response = dd.cygnet_consent_verify_ewb_credentials(consent_request_data)
        # ss = dd.cygnet_verify_ewb_credentials("")
        ss = dd.cygnet_get_ewb_details(get_ewaybill_bulk_request)
        # ss = dd.get_auth_token()
        # ss = dd.cygnet_create_customer(request_data)
    except Exception as e:
        logger.error(f"getting error {e}")


def validate_bulk_invoice_data_formate(group_data, db, merchant_key):
    logger.info(f"getting request group packet {group_data}")
    # grouping_ids = set()
    for single_request_data in group_data:
        logger.info(f"getting request single_request_data packet {single_request_data}")
        invoice_suffix_response = utils.check_invoice_suffix(single_request_data)
        logger.info(f"invoice_suffix_response >>> {invoice_suffix_response}")
        if not invoice_suffix_response:
            return {**ErrorCodes.get_error_response(1139)}

        invoice_date_response = utils.check_inv_date(single_request_data)
        logger.info(f"invoice_date_response >>>> >>> {invoice_date_response}")
        if not invoice_date_response:
            return {**ErrorCodes.get_error_response(1140)}

        duplicate_invoice_num_response = utils.duplicate_inv_no(db, single_request_data)
        logger.info(f"duplicate_invoice_num_response >>>> >>> {duplicate_invoice_num_response}")
        if duplicate_invoice_num_response:
            return {**ErrorCodes.get_error_response(1141)}

        special_char_remove_invoice_num_response = SpecialCharRemove.special_chr_remove_inv_no(
            db,
            single_request_data,
            merchant_key
        )
        if special_char_remove_invoice_num_response.get("code") != 200:
            return {
                **special_char_remove_invoice_num_response
            }

        # current_grouping_id = single_request_data["groupingId"]

        # if current_grouping_id in grouping_ids:
        #     print(f"Error: Duplicate groupingId found - {current_grouping_id}")
        #     return {**ErrorCodes.get_error_response(1099)}
        # else:
        #     grouping_ids.add(current_grouping_id)

    logger.info(f"Validation complete. No duplicate groupingId found. invoice data")
    return {**ErrorCodes.get_error_response(200)}


def validate_bulk_invoice_date(group_data):
    invoice_bulk_date_response = utils.check_bulk_inv_date(group_data)
    logger.info(f"invoice_bulk_date_response >>>> >>> {invoice_bulk_date_response}")
    if not invoice_bulk_date_response:
        return {**ErrorCodes.get_error_response(1140)}
    return {**ErrorCodes.get_error_response(200)}
