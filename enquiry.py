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
from schema import InvoiceRequestSchema, FinanceSchema, CancelLedgerSchema, CheckStatusSchema, CheckEnquirySchema, AsyncInvoiceRegistrationWithCodeSchema, AsyncValidationServiceWithCodeSchema, AsyncValidationServiceWithoutCodeSchema
import views
from errors import ErrorCodes
from utils import get_financial_year, check_invoice_date, check_invoice_due_date
from utils import generate_key_secret, create_post_processing, validate_signature, SpecialCharRemove
from config import webhook_task
from enquiry_view import AsyncValidationServiceWithCode, AsyncValidationServiceWithoutCode
from database import get_db
from sqlalchemy import desc, text

logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)


class TagsMetadata:
    async_validation_service_with_code = {
        "name": "async validation service with entity code",
        "description": "**Aim** : Before establishing the invoice registration service with the entity code called,"
                       "this API will be made available to verify whether the format of all requested data is correct"
                       "or not in the validation service with the entity code format via webhook.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : APIRequestLog, PostProcessingRequest, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, EntityCombination\n"
                      "\n **Function** :validate_signature, create_request_log, create_post_processing, \n"
                      "validation_service_with_code, check_entity_code_gst_validation, webhook_task",
        "summary": "Summary of asyncValidationServiceWithCode"
    }
    sync_validation_service_with_code = {
        "name": "sync validation service with entity code",
        "description": "**Aim** :Before establishing the invoice registration service with the entity code called,"
                       "this API will be made available to verify whether the format of all requested data is correct"
                       "or not in the validation service with the entity code format in response.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : APIRequestLog, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, EntityCombination\n"
                       "\n **Function** :validate_signature, create_request_log, sync_validation_service_with_code, \n"
                       "check_entity_code_gst_validation",
        "summary": "Summary of syncValidationServiceWithEntityCode"
    }
    async_validation_service_without_code = {
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
                       "\n **Table** : APIRequestLog, PostProcessingRequest, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, EntityCombination\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, validation_service_without_code, \n"
                       "check_entity_code_gst_validation, webhook_task",
        "summary": "Summary of asyncValidationServiceWithoutCode"
    }
    sync_validation_service_without_code = {
        "name": "async validation service with entityout code",
        "description": "**Aim** : Before establishing the invoice registration service without the entity code called, "
                       "this API will be made available to verify whether the format of all requested data is correct "
                       "or not in the validation service for invoices without entity code format in response.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- Date format validation\n"
                       "- Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : APIRequestLog, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, EntityCombination\n"
                       "\n **Function** : create_request_log, validate_signature, sync_validation_service_without_code, \n"
                       "check_entity_code_gst_validation",
        "summary": "Summary of syncValidationServiceWithoutCode"
    }
    enquiry_api = {
        "name": "Enquiry API signature",
        "description": "**Aim** : This Enquiry API will be provided to check the status of the requested API for which a response was not yet sent via Webhook..\n"
                       "\n**Validation** : \n"
                       "- requestId format validation\n"
                       "\n **Table** :  MerchantDetails, PostProcessingRequest\n"
                       "\n **Function** : check_enquiry_api",
        "summary": "Summary of enquiry"
    }
    cancel_code = {
        "name": "Marking Ledger as Cancellation",
        "description": "**Aim** : The IDP will use this API to change the status of a funded ledger ID to "
                       "unfunded with a cancellation reason.\n"
                       "\n**Validation** : \n"
                       "- Ledger should be in financed state\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "- Ledger should exist\n"
                       "\n **Table** : APIRequestLog, MerchantDetails, Ledger\n"
                       "\n **Function** : create_request_log, validate_signature, cancel, \n"
                       "check_ledger.",
        "summary": "Summary of cancellation"
    }

# old enquiry api
# @router.post("/enquiry/{merchant_key}",
#              description=TagsMetadata.enquiry_api.get('description'))
# def check_enquiry_api(merchant_key: str,
#                       request: CheckEnquirySchema,
#                       db: Session = Depends(get_db)):
#
#     merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
#     if merchant_details:
#         request_id = db.query(
#             models.PostProcessingRequest
#         ).filter(models.PostProcessingRequest.request_extra_data.contains({
#                 "requestId": request.requestId
#             })).first()
#
#         if request_id:
#             logger.info(f"getting enquiry response webhook data >>>>>>>>>>>>>>>> {request_id.webhook_response}")
#             if request_id.webhook_response:
#                 return {
#                     **request_id.webhook_response
#                 }
#             else:
#                 return {"requestId": request.requestId, **ErrorCodes.get_error_response(1057)}
#         else:
#             return {"requestId": request.requestId, **ErrorCodes.get_error_response(1024)}
#     else:
#         return {"requestId": request.requestId, **ErrorCodes.get_error_response(1002)}


@router.post("/enquiry/{merchant_key}",
             description=TagsMetadata.enquiry_api.get('description'))
def check_enquiry_api(merchant_key: str,
                      request: CheckEnquirySchema,
                      db: Session = Depends(get_db)):

    merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
    merchant_id = merchant_details.id if merchant_details else ''
    merchant_idp_id = str(merchant_id)
    if merchant_details:
        query = """
                    SELECT 
                        request_extra_data->>'requestId' as req_id ,
                        webhook_response AS webhook_response
                    FROM 
                        post_processing_request ppr
                    WHERE 
                        ppr.merchant_id = :merchant_id AND ppr.request_extra_data->>'requestId' = :requestId 
                ;
                """
        param = {
            'merchant_id': merchant_idp_id,
            'requestId': request.requestId
        }

        enquiry_req_data = db.execute(text(query), param).first()

        if enquiry_req_data is None:
            # API REQUEST LOG RESPONSE DATA
            api_request_obj = db.query(models.APIRequestLog).filter(models.APIRequestLog.request_id == request.requestId
            ).first()
            if api_request_obj is None:
                return {"requestId": request.requestId, **ErrorCodes.get_error_response(1024)}
            api_response_data = api_request_obj.response_data
            if api_response_data:
                return {**api_response_data}
            else:
                return {"requestId": request.requestId, **ErrorCodes.get_error_response(1151)}

        # POST PROCESSING REQUEST WEBHOOK RESPONSE DATA
        if enquiry_req_data.webhook_response:
            webhook_resp = enquiry_req_data.webhook_response
            return {
                **webhook_resp
            }
        else:
            return {"requestId": request.requestId, **ErrorCodes.get_error_response(1057)}

    else:
        return {"requestId": request.requestId, **ErrorCodes.get_error_response(1002)}


@router.post("/validation-service-with-code/{merchant_key}",
             description=TagsMetadata.async_validation_service_with_code.get('description'))
# "async_invoice_registration_with_code",
def async_validation_service_with_code_api(merchant_key: str,
                      request:  AsyncInvoiceRegistrationWithCodeSchema,
                      db: Session = Depends(get_db)):

    api_request_log_res = utils.create_request_log(db, request.requestId, jsonable_encoder(request), '', 'request','async-validation-service-with-code', merchant_key)
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }

    # validation invoice number, date , duplicate check and amount
    invoice_suffix_response = utils.check_invoice_suffix(jsonable_encoder(request))
    logger.info(f"invoice_suffix_response >>> {invoice_suffix_response}")
    if not invoice_suffix_response:
        return {**ErrorCodes.get_error_response(1139)}

    invoice_date_response = utils.check_inv_date(jsonable_encoder(request))
    logger.info(f"invoice_date_response >>>> >>> {invoice_date_response}")
    if not invoice_date_response:
        return {**ErrorCodes.get_error_response(1140)}

    duplicate_invoice_num_response = utils.duplicate_inv_no(db, jsonable_encoder(request))
    logger.info(f"duplicate_invoice_num_response >>>> >>> {duplicate_invoice_num_response}")
    if duplicate_invoice_num_response:
        return {**ErrorCodes.get_error_response(1141)}

    special_char_remove_invoice_num_response = SpecialCharRemove.special_chr_remove_inv_no(
        db,
        jsonable_encoder(request),
        merchant_key
    )

    if special_char_remove_invoice_num_response.get("code") != 200:
        return {
            "requestId": request.requestId,
            **special_char_remove_invoice_num_response
        }

    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = create_post_processing(
            db,
            jsonable_encoder(request),
            "async_validation_service_with_code",
            'request',
            merchant_key,
            "successfully added in post processing"
        )
        webhook_task.delay(jsonable_encoder(request), merchant_key, 'validation_service_with_code')
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

# old
# @router.post("/validation-service-without-code/{merchant_key}",
#              description=TagsMetadata.async_validation_service_without_code.get('description'))
# # "async_invoice_registration_without_code",
# def validation_service_without_code_api(merchant_key: str,
#                       request:  InvoiceRequestSchema,
#                       db: Session = Depends(get_db)):
#
#     json_data = jsonable_encoder(request)
#     if not json_data.get('sellerIdentifierData'):
#         json_data.pop("sellerIdentifierData")
#     if not json_data.get('buyerIdentifierData'):
#         json_data.pop("buyerIdentifierData")
#     validate_signature_response = validate_signature(db, json_data, merchant_key)
#     if validate_signature_response.get('code') != 200:
#         api_request_log_res = views.create_request_log(db, request.requestId, jsonable_encoder(request), '', 'request', 'async-validation-service-without-code')
#         if api_request_log_res.get("code") != 200:
#             return {
#                 "requestId": request.requestId,
#                 **api_request_log_res
#             }
#         views.create_request_log(db, request.requestId, '', validate_signature_response, 'response')
#         return validate_signature_response
#
#     seller_pan_response = utils.validate_seller_invoice_pan_gst_pan(jsonable_encoder(request))
#     buyer_pan_response = utils.validate_buyer_invoice_pan_gst_pan(jsonable_encoder(request))
#     if seller_pan_response:
#         return {**ErrorCodes.get_error_response(1060)}
#     if buyer_pan_response:
#         return {**ErrorCodes.get_error_response(1059)}
#
#     return_response = create_post_processing(
#         db,
#         jsonable_encoder(request),
#         "async_validation_service_without_code",
#         'request',
#         merchant_key,
#         "successfully added in post processing"
#     )
#     webhook_task.delay(jsonable_encoder(request), merchant_key, 'validation_service_without_code')
#         # AsyncValidationServiceWithoutCode.validation_service_without_code(
#         #     db,
#         #     jsonable_encoder(request),
#         #     merchant_key
#         # )
#     return return_response


@router.post("/validation-service-without-code/{merchant_key}",
             description=TagsMetadata.async_validation_service_without_code.get('description'))
# "async_invoice_registration_without_code",
def async_validation_service_without_code_api(merchant_key: str,
                      request:  InvoiceRequestSchema,
                      db: Session = Depends(get_db)):

    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'async-validation-service-without-code',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }

    seller_pan_response = utils.validate_seller_invoice_pan_gst_pan(jsonable_encoder(request))
    buyer_pan_response = utils.validate_buyer_invoice_pan_gst_pan(jsonable_encoder(request))
    if seller_pan_response:
        return {**ErrorCodes.get_error_response(1060)}
    if buyer_pan_response:
        return {**ErrorCodes.get_error_response(1059)}

    json_data = jsonable_encoder(request)
    if not json_data.get('sellerIdentifierData'):
        if json_data.get('sellerGst') == "":
            return {**ErrorCodes.get_error_response(1067)}
        json_data.pop("sellerIdentifierData")
    if not json_data.get('buyerIdentifierData'):
        if json_data.get('buyerGst') == "":
            return {**ErrorCodes.get_error_response(1068)}
        json_data.pop("buyerIdentifierData")

    # validation invoice number, date , duplicate check and amount
    invoice_suffix_response = utils.check_invoice_suffix(jsonable_encoder(request))
    logger.info(f"invoice_suffix_response >>> {invoice_suffix_response}")
    if not invoice_suffix_response:
        return {**ErrorCodes.get_error_response(1139)}

    invoice_date_response = utils.check_inv_date(jsonable_encoder(request))
    logger.info(f"invoice_date_response >>>> >>> {invoice_date_response}")
    if not invoice_date_response:
        return {**ErrorCodes.get_error_response(1140)}

    duplicate_invoice_num_response = utils.duplicate_inv_no(db, jsonable_encoder(request))
    logger.info(f"duplicate_invoice_num_response >>>> >>> {duplicate_invoice_num_response}")
    if duplicate_invoice_num_response:
        return {**ErrorCodes.get_error_response(1141)}

    special_char_remove_invoice_num_response = SpecialCharRemove.special_chr_remove_inv_no(
        db,
        jsonable_encoder(request),
        merchant_key
    )
    if special_char_remove_invoice_num_response.get("code") != 200:
        return {
            "requestId": request.requestId,
            **special_char_remove_invoice_num_response
        }

    validate_signature_response = validate_signature(db, json_data, merchant_key)
    # validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = create_post_processing(
            db,
            jsonable_encoder(request),
            "async_validation_service_without_code",
            "request",
            merchant_key,
            "successfully added in post processing"
        )
        webhook_task.delay(jsonable_encoder(request), merchant_key, 'validation_service_without_code')
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


@router.post("/sync-validation-service-with-code/{merchant_key}",
             description=TagsMetadata.sync_validation_service_with_code.get('description'))
def sync_validation_service_with_code(merchant_key: str, request: AsyncInvoiceRegistrationWithCodeSchema,
                                        db: Session = Depends(get_db)):

    # validation invoice number, date , duplicate check and amount
    validate_inv_data_response = validate_invoice_data_formate(jsonable_encoder(request), db, merchant_key)
    if validate_inv_data_response.get('code') != 200:
        return {
            "requestId": request.requestId,
            **validate_inv_data_response
        }

    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = AsyncValidationServiceWithCode.sync_validation_service_with_code(
            db,
            jsonable_encoder(request),
            merchant_key,
        )
    else:
        return_response = validate_signature_response
    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_request_log(db, request.requestId, '', return_response, 'response')
    return return_response


@router.post("/sync-validation-service-without-code/{merchant_key}",
            description=TagsMetadata.sync_validation_service_without_code.get('description'))
def sync_validation_service_without_code(merchant_key: str, request: InvoiceRequestSchema,
                                        db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'sync-validation-service-without-code',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    seller_pan_response = utils.validate_seller_invoice_pan_gst_pan(jsonable_encoder(request))
    buyer_pan_response = utils.validate_buyer_invoice_pan_gst_pan(jsonable_encoder(request))
    if seller_pan_response:
        return {**ErrorCodes.get_error_response(1060)}
    if buyer_pan_response:
        return {**ErrorCodes.get_error_response(1059)}

    json_data = jsonable_encoder(request)
    # if not json_data.get('sellerIdentifierData'):
    #     json_data.pop("sellerIdentifierData")
    # if not json_data.get('buyerIdentifierData'):
    #     json_data.pop("buyerIdentifierData")

    if not json_data.get('sellerIdentifierData'):
        if json_data.get('sellerGst') == "":
            return {**ErrorCodes.get_error_response(1067)}
        json_data.pop("sellerIdentifierData")
    if not json_data.get('buyerIdentifierData'):
        if json_data.get('buyerGst') == "":
            return {**ErrorCodes.get_error_response(1068)}
        json_data.pop("buyerIdentifierData")

    #validation invoice number, date , duplicate check and amount
    invoice_suffix_response = utils.check_invoice_suffix(jsonable_encoder(request))
    logger.info(f"invoice_suffix_response >>> {invoice_suffix_response}")
    if not invoice_suffix_response:
        return {**ErrorCodes.get_error_response(1139)}

    # sorted_inv_data = sorted(json_data["ledgerData"], key=lambda x: x.get("invoiceNo"))
    # logger.info(f"sorted_list---{sorted_inv_data}")

    invoice_date_response = utils.check_inv_date(jsonable_encoder(request))
    logger.info(f"invoice_date_response >>>> >>> {invoice_date_response}")
    if not invoice_date_response:
        return {**ErrorCodes.get_error_response(1140)}

    duplicate_invoice_num_response = utils.duplicate_inv_no(db, jsonable_encoder(request))
    logger.info(f"duplicate_invoice_num_response >>>> >>> {duplicate_invoice_num_response}")
    if duplicate_invoice_num_response:
        return {**ErrorCodes.get_error_response(1141)}

    special_char_remove_invoice_num_response = SpecialCharRemove.special_chr_remove_inv_no(
        db,
        jsonable_encoder(request),
        merchant_key
    )
    # if special_char_remove_invoice_num_response.get('code') != 200:
    #     return special_char_remove_invoice_num_response

    if special_char_remove_invoice_num_response.get("code") != 200:
        return {
            "requestId": request.requestId,
            **special_char_remove_invoice_num_response
        }

    validate_signature_response = validate_signature(db, json_data, merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = AsyncValidationServiceWithoutCode.sync_validation_service_without_code(
            db,
            jsonable_encoder(request),
            merchant_key,
        )
    else:
        return_response = validate_signature_response
    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_request_log(db, request.requestId, '', return_response, 'response')
    return return_response


@router.post("/cancel/{merchant_key}", description=TagsMetadata.cancel_code.get('description'))
def cancel_fund_ledger(merchant_key: str, request: CancelLedgerSchema, db: Session = Depends(get_db)):
    api_request_log_res = views.create_request_log(db, request.requestId, jsonable_encoder(request), '', 'request', 'cancel', merchant_key)
    if api_request_log_res.get("code") == 200:
        validate_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
        if validate_response.get('code') == 200:
            return_response = views.CancelLedger.cancel(
                db,
                request,
                validate_response.get('merchant_details')
            )
            views.create_request_log(db, request.requestId, '', return_response, 'response')
            return return_response
        else:
            views.create_request_log(db, request.requestId, '', validate_response, 'response')
            return validate_response
    else:
        return api_request_log_res


def validate_invoice_data_formate(request_data, db, merchant_key):
    logger.info(f"getting request group packet {request_data}")
    invoice_suffix_response = utils.check_invoice_suffix(request_data)
    logger.info(f"invoice_suffix_response >>> {invoice_suffix_response}")
    if not invoice_suffix_response:
        return {**ErrorCodes.get_error_response(1139)}

    invoice_date_response = utils.check_inv_date(request_data)
    logger.info(f"invoice_date_response >>>> >>> {invoice_date_response}")
    if not invoice_date_response:
        return {**ErrorCodes.get_error_response(1140)}

    duplicate_invoice_num_response = utils.duplicate_inv_no(db, request_data)
    logger.info(f"duplicate_invoice_num_response >>>> >>> {duplicate_invoice_num_response}")
    if duplicate_invoice_num_response:
        return {**ErrorCodes.get_error_response(1141)}

    special_char_remove_invoice_num_response = SpecialCharRemove.special_chr_remove_inv_no(
        db,
        request_data,
        merchant_key
    )
    if special_char_remove_invoice_num_response.get("code") != 200:
        return {
            **special_char_remove_invoice_num_response
        }

    logger.info(f"Validation complete. No duplicate found. in invoice data")
    return {**ErrorCodes.get_error_response(200)}
