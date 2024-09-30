import asyncio
import time
import logging
# import schemas
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
    InvoiceBulkRequestSchema
from utils import generate_key_secret, create_post_processing, validate_signature, create_request_log
from models import MerchantDetails
from registration_view import AsyncInvoiceRegistrationCode, AsyncEntityRegistration, AsyncRegistration
from config import webhook_task

logger = logging.getLogger(__name__)
router = APIRouter()


class TagsMetadata:
    async_invoice_registration_withoutcode = {
        "name": "Async Invoice Registration Without Entity Code",
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
                       "\n **Table** : APIRequestLog, PostProcessingRequest, Invoice, Ledger, MerchantDetails, "
                       "EntityIdentifierLine, Entity, InvoiceEncryptedData.\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, "
                       "create_invoice_post \n"
                       "check_entity_code_gst_validation, update_existing_seller_identifier,  "
                       "update_existing_buyer_identifier, "
                       "add_buyer_identifier_lines, create_buyer_identifier_conditionaly, "
                       "add_seller_identifier_lines, create_seller_identifier_conditionaly, create_invoice, "
                       "create_ledger, "
                       "check_buyer_identifier_data, create_ledger_hash_and_validate, validate_ledger_hash,"
                       " check_entity_code_gst_validation, webhook_task.",
        "summary": "Summary of asyncValidationServiceWithoutCode"
    }
    sync_invoice_registration_withoutcode = {
        "name": "Sync Invoice Registration Without Entity Code",
        "description": "**Aim** : This API for invoice registration does not require entity code. IDP will request to"
                       " register the invoice/ group of invoices against themselves, and a ledger ID created"
                       " and response will be returned..\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : APIRequestLog, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, Entity, "
                       "InvoiceEncryptedData.\n"
                       "\n **Function** : create_request_log, validate_signature, create_invoice_post \n"
                       "check_entity_code_gst_validation, update_existing_seller_identifier,  "
                       "update_existing_buyer_identifier, add_buyer_identifier_lines, "
                       "create_buyer_identifier_conditionaly, add_seller_identifier_lines, "
                       "create_seller_identifier_conditionaly, create_invoice, create_ledger, "
                       "check_buyer_identifier_data, create_ledger_hash_and_validate, validate_ledger_hash,"
                       " check_entity_code_gst_validation.",
        "summary": "Summary of asyncValidationServiceWithoutCode"
    }
    async_invoice_registration_withcode = {
        "name": "Async Invoice Registration With Entity Code",
        "description": "**Aim** : This API for invoice registration require to be with buyer and seller entities are "
                       "already pre registered. IDP will request to register the invoice/ group of invoices against "
                       "themselves, and a ledger ID created.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : APIRequestLog,PostProcessingRequest, Invoice, Ledger, MerchantDetails, "
                       "EntityIdentifierLine, EntityCombination, Entity, InvoiceEncryptedData.\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, "
                       "create_invoice_post \n"
                       "check_entity_code_gst_validation, check_create_derived_pan_seller, "
                       "check_create_derived_pan_buyer, "
                       " create_invoice, create_ledger_hash_and_validate, create_ledger, "
                       "webhook_task.",
        "summary": "Summary of asyncValidationServiceWithCode"
    }
    sync_invoice_registration_withcode = {
        "name": "Sync Invoice Registration With Entity Code",
        "description": "**Aim** : This API for invoice registration require to be with buyer and seller entities are "
                       "already pre registered. IDP will request to register the invoice/ group of invoices against "
                       "themselves, and a ledger ID created via a Webhook-based response will be returned.\n"
                       "\n**Validation** : \n"
                       "- GST format validation\n"
                       "- RequestId format validation\n"
                       "- Date format validation\n"
                       "- IDP ID and invoice data combo uniqueness validation\n"
                       "- Invoice date should not be greater than current date\n"
                       "\n **Table** : APIRequestLog, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, "
                       "EntityCombination, Entity, InvoiceEncryptedData\n"
                       "\n **Function** : create_request_log, validate_signature, create_invoice_post \n"
                       "check_entity_code_gst_validation, check_create_derived_pan_seller, "
                       "check_create_derived_pan_buyer, "
                       " create_invoice, create_ledger_hash_and_validate, create_ledger.",
        "summary": "Summary of asyncValidationServiceWithCode"
    }
    async_entity_registration_code = {
        "name": "Async Entity registration API",
        "description": "**Aim** : The registration of the entity (seller/buyer) is requested by this API for a specific"
                       " IDP created via Webhook-based response will be returned.\n"
                       "\n**Validation** : \n"
                       "- requestId format validation\n"
                       "- Entity Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "\n **Table** :  APIRequestLog, PostProcessingRequest, MerchantDetails, EntityIdentifierLine,"
                       " EntityCombination, Entity\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing,"
                       "create_entity_post, create_entity, webhook_task.",
        "summary": "Summary of asyncEntityRegistration"
    }
    sync_entity_registration_code = {
        "name": "Sync Entity registration API",
        "description": "**Aim** : The registration of the entity (seller/buyer) is requested by this API for a specific"
                       " IDP created.\n"
                       "\n**Validation** : \n"
                       "- requestId format validation\n"
                       "- Entity Identification data validation\n"
                       "- If id type is bank a/c, IFSC code should be provided\n"
                       "- LEI / PAN / CIN format validation\n"
                       "\n **Table** :APIRequestLog, MerchantDetails, EntityIdentifierLine,"
                       " EntityCombination, Entity\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing,"
                       "create_entity_post, create_entity. ",
        "summary": "Summary of syncEntityRegistration"
    }


@router.post("/async-registration-without-code/{merchant_key}",
             description=TagsMetadata.async_invoice_registration_withoutcode.get('description'))
def async_registration_without_code(merchant_key: str, request: InvoiceRequestSchema, db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'async-registration-without-code',
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

    validate_signature_response = validate_signature(db, json_data, merchant_key)
    # validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = create_post_processing(
            db,
            jsonable_encoder(request),
            "async_registration_without_code",
            "request",
            merchant_key,
            "successfully added in post processing"
        )
        webhook_task.delay(jsonable_encoder(request), merchant_key, 'async_invoice_registration')
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


@router.post("/async-entity-registration/{merchant_key}",
             description=TagsMetadata.async_entity_registration_code.get('description'))
def async_entity_registration(merchant_key: str, request: EntityRegistrationSchema, db: Session = Depends(get_db)):
    json_data = jsonable_encoder(request)
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'async-entity-registration',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    # derived_pan_response = utils.validate_pan_gst_pan(jsonable_encoder(request))
    check_gst_duplication = utils.check_for_entity_gst_duplicate_values(json_data)
    if not check_gst_duplication:
        return {"requestId": request.requestId, **ErrorCodes.get_error_response(1058)}
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = create_post_processing(
            db,
            jsonable_encoder(request),
            "async_entity_registration",
            "request",
            merchant_key,
            "successfully added in post processing"
        )
        webhook_task.delay(jsonable_encoder(request), merchant_key, 'entity_registration')
    else:
        return_response = validate_signature_response
    return return_response


@router.post("/async-invoice-registration-with-code/{merchant_key}",
             description=TagsMetadata.async_invoice_registration_withcode.get('description'))
def async_invoice_registration_with_code(merchant_key: str, request: AsyncInvoiceRegistrationWithCodeSchema,
                                         db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'async-invoice-registration-with-code',
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
                "async_invoice_registration_with_code",
                'request',
                merchant_key,
                "successfully added in post processing"
            )
            webhook_task.delay(jsonable_encoder(request), merchant_key, 'invoice_registration_code')
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


@router.post("/sync-invoice-registration-with-code/{merchant_key}",
             description=TagsMetadata.sync_invoice_registration_withcode.get('description'))
def sync_invoice_registration_with_code(merchant_key: str, request: AsyncInvoiceRegistrationWithCodeSchema,
                                        db: Session = Depends(get_db)):
    json_data = jsonable_encoder(request)
    api_request_log_res = utils.create_request_log(
        db,
        json_data.get('requestId'),
        jsonable_encoder(request),
        '',
        'request',
        'sync-invoice-registration-with-code',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": json_data.get('requestId'),
            **api_request_log_res
        }
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = AsyncInvoiceRegistrationCode.create_invoice_post(
            db,
            jsonable_encoder(request),
            merchant_key,
            'sync'
        )
    else:
        return_response = validate_signature_response
    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_request_log(db, json_data.get('requestId'), '', return_response, 'response')
    utils.create_request_log(
        db,
        json_data.get('requestId'),
        '',
        return_response,
        'response'
    )
    return return_response


def validate_seller_buyer_code(request_data, merchant_obj, db):
    seller_code = request_data.get('sellerCode')
    buyer_code = request_data.get('buyerCode')
    seller_gst = request_data.get('sellerGst')
    buyer_gst = request_data.get('buyerGst')
    if seller_code == buyer_code:
        return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1021)}

    if seller_gst and buyer_gst:
        if request_data.get('sellerGst') == request_data.get('buyerGst'):
            return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1022)}

    buyer_entity_combination_obj = db.query(
        models.EntityCombination
    ).filter(
        models.EntityCombination.merchant_id == merchant_obj.id,
        models.EntityCombination.entity_code == buyer_code,
        models.EntityCombination.is_active == True
    ).first()

    seller_entity_combination_obj = db.query(
        models.EntityCombination
    ).filter(
        models.EntityCombination.merchant_id == merchant_obj.id,
        models.EntityCombination.entity_code == seller_code,
        models.EntityCombination.is_active == True
    ).first()

    if not buyer_entity_combination_obj:
        return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1020)}

    if not seller_entity_combination_obj:
        return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1018)}

    return {"code": 200}


@router.post("/sync-registration-without-code/{merchant_key}",
             description=TagsMetadata.sync_invoice_registration_withoutcode.get('description'))
def sync_registration_without_code(merchant_key: str, request: InvoiceRequestSchema, db: Session = Depends(get_db)):
    json_data = jsonable_encoder(request)
    api_request_log_res = utils.create_request_log(
        db,
        json_data.get('requestId'),
        json_data,
        '',
        'request',
        'sync-registration-without-code',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": json_data.get('requestId'),
            **api_request_log_res
        }
    seller_pan_response = utils.validate_seller_invoice_pan_gst_pan(json_data)
    buyer_pan_response = utils.validate_buyer_invoice_pan_gst_pan(json_data)
    if seller_pan_response:
        return {**ErrorCodes.get_error_response(1060)}
    if buyer_pan_response:
        return {**ErrorCodes.get_error_response(1059)}
    # json_data = jsonable_encoder(request)
    if not json_data.get('sellerIdentifierData'):
        if json_data.get('sellerGst') == "":
            return {**ErrorCodes.get_error_response(1067)}
        json_data.pop("sellerIdentifierData")
    if not json_data.get('buyerIdentifierData'):
        if json_data.get('buyerGst') == "":
            return {**ErrorCodes.get_error_response(1068)}
        json_data.pop("buyerIdentifierData")

    validate_signature_response = validate_signature(db, json_data, merchant_key)
    if validate_signature_response.get('code') == 200:
        return_response = AsyncRegistration.create_invoice_post(
            db,
            jsonable_encoder(request),
            merchant_key,
            'sync'
        )
    else:
        return_response = validate_signature_response

    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_request_log(db, json_data.get('requestId'), '', return_response, 'response')
    utils.create_request_log(
        db,
        json_data.get('requestId'),
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/sync-entity-registration/{merchant_key}",
             description=TagsMetadata.sync_entity_registration_code.get('description'))
def sync_entity_registration(merchant_key: str, request: EntityRegistrationSchema, db: Session = Depends(get_db)):
    json_data = jsonable_encoder(request)
    api_request_log_res = utils.create_request_log(
        db,
        json_data.get('requestId'),
        jsonable_encoder(request),
        '',
        'request',
        'sync-entity-registration',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": json_data.get('requestId'),
            **api_request_log_res
        }

    check_gst_duplication = utils.check_for_entity_gst_duplicate_values(json_data)
    if not check_gst_duplication:
        return {"requestId": json_data.get('requestId'), **ErrorCodes.get_error_response(1058)}
    # derived_pan_response = utils.validate_pan_gst_pan(jsonable_encoder(request))
    # if derived_pan_response:
    #     return {**ErrorCodes.get_error_response(1058)}
    validate_signature_response = validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:

        return_response = AsyncEntityRegistration.create_entity_post(
            db,
            jsonable_encoder(request),
            merchant_key,
            'sync'
        )
    else:
        return_response = validate_signature_response
    response_hash = utils.create_response_hash(db, return_response, merchant_key)
    return_response.update({"signature": response_hash})
    utils.create_request_log(db, json_data.get('requestId'), '', return_response, 'response')
    utils.create_request_log(
        db,
        json_data.get('requestId'),
        '',
        return_response,
        'response'
    )
    return return_response


@router.post("/async-bulk-registration-without-codes/{merchant_key}",
             description=TagsMetadata.async_invoice_registration_withoutcode.get('description'))
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
                # return_response = AsyncRegistration.create_bulk_invoice_post(db, request_data, merchant_key)
            else:
                return_response = {
                    "requestId": request_data.get('requestId'),
                    **validate_bulk_response
                }
        else:
            validate_signature_response.pop('merchant_details')
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

    return {**ErrorCodes.get_error_response(200)}


def validate_grouping_id(request_group_data):
    grouping_ids = set()

    for group in request_group_data:
        current_grouping_id = group["groupingId"]

        if current_grouping_id in grouping_ids:
            print(f"Error: Duplicate groupingId found - {current_grouping_id}")
            return {**ErrorCodes.get_error_response(1067)}
        else:
            grouping_ids.add(current_grouping_id)

    print("Validation complete. No duplicate groupingId found.")
    return {**ErrorCodes.get_error_response(200)}
