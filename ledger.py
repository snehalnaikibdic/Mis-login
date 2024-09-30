import copy
import logging
import time
import redis
import ast
import io
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.encoders import jsonable_encoder
from decouple import config as dconfig
import requests
import json
import csv
import models
from database import SessionLocal
from sqlalchemy.orm import Session
import utils
from models import MerchantDetails
from routers.auth import get_current_merchant_active_user, User

from schema import InvoiceRequestSchema, FinanceSchema, CancelLedgerSchema, CheckStatusSchema, AsyncFinanceSchema, \
    AsyncDisburseSchema, AsyncRepaymentSchema, EntityRegistrationSchema
import views
from errors import ErrorCodes
from utils import get_financial_year, check_invoice_date
from database import get_db

# logging.config.fileConfig('logging.conf', disable_existing_loggers=False)

logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)


class TagsMetadata:
    cancel_code = {
        "name": "Marking Ledger as Cancellation",
        "description": "**Aim** : The IDP will use this API to change the status of a funded ledger ID/ invoice ID to "
                       "unfunded with a cancellation reason.\n"
                       "\n**Validation** : \n"
                       "- Ledger should be in financed state\n"
                       "- Ledger should have been registered by the requesting IDP\n"
                       "- Ledger should exist\n"
                       "\n **Table** : CancelLedger, InvoiceStatus, Ledger, MerchantDetails\n"
                       "\n **Function** : create_request_log, validate_signature, create_post_processing, cancel, "
                       "cancel_ledger, \n"
                       "check_ledger.",
        "summary": "Summary of cancellation"
    }


@router.post("/registration/{merchant_key}")
def invoice_register(
        merchant_key: str,
        request: InvoiceRequestSchema,
        current_user: Annotated[User, Depends(get_current_merchant_active_user)],
        db: Session = Depends(get_db),
):
    financial_year = get_financial_year()
    invoice_date_response = check_invoice_date(jsonable_encoder(request))
    start_time = time.time()
    duplicates_json_exist = utils.are_duplicates_exist(jsonable_encoder(request).get('ledgerData'))
    logger.info(f"getting duplicates json exist >>>>>>>>>>>>>>>>>> {duplicates_json_exist}")
    duplicates_exist = utils.check_for_duplicate_values(jsonable_encoder(request).get('ledgerData'))
    logger.info(f"getting duplicates gst exist >>>>>>>>>>>>>>>>>> {duplicates_exist}")
    api_request_log_res = views.create_request_log(db, request.requestId, jsonable_encoder(request), '', 'request')
    if api_request_log_res.get("code") == 200:
        if not duplicates_json_exist:
            if not duplicates_exist:
                if invoice_date_response:
                    validate_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
                    if validate_response.get('code') == 200:
                        return_response = views.Registration.create_invoice(
                            db,
                            jsonable_encoder(request),
                            validate_response.get('merchant_details'),
                            financial_year
                        )
                        end_time = time.time()
                        time_taken = end_time - start_time
                        logger.info(f"time taken for registration >>>>>>>>>>>>>>>>>>> {time_taken}")
                        views.create_request_log(db, request.requestId, '', return_response, 'response')
                        return return_response
                    else:
                        views.create_request_log(db, request.requestId, '', validate_response, 'response')
                        return validate_response
                else:
                    return {**ErrorCodes.get_error_response(1016)}
            else:
                return {**ErrorCodes.get_error_response(1015)}
        else:
            return {**ErrorCodes.get_error_response(1014)}
    else:
        return api_request_log_res


@router.post("/financing/{merchant_key}")
def finance(merchant_key: str, request: FinanceSchema, db: Session = Depends(get_db)):
    financial_year = get_financial_year()
    api_request_log_res = views.create_request_log(db, request.requestId, jsonable_encoder(request), '', 'request')
    if api_request_log_res.get("code") == 200:
        validate_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
        logger.info(f"getting valdiate response >>>>>>>>>>>>>>>>>>>>>>>>>>>>. {validate_response}")
        cache_response = get_cache(jsonable_encoder(request.ledgerData))
        logger.info(f"getting cache response %%%%%%%%%%%%%%%%%%%%%%%%%%%. {cache_response}")
        if validate_response.get('code') == 200:
            if cache_response.get('code') == 200:
                return_response = views.Financing.fund_ledger(
                    db,
                    request,
                    validate_response.get('merchant_details'),
                    cache_response.get('invoicesList'),
                    financial_year
                )
                views.create_request_log(db, request.requestId, '', return_response, 'response')
                return return_response
            else:
                response_hash = utils.create_ledger_hash(
                    cache_response,
                    validate_response.get('merchant_details').merchant_secret
                )
                cache_response.update({"signature": response_hash})
                return cache_response
        else:
            views.create_request_log(db, request.requestId, '', validate_response, 'response')
            return validate_response
        # if cache_response.get('code') == 200:
        #     if validate_response.get('code') == 200:
        #         return_response = views.Financing.fund_ledger(
        #             db,
        #             request,
        #             validate_response.get('merchant_details')
        #         )
        #         views.create_request_log(db, request.requestId, '', return_response, 'response')
        #         return return_response
        #     else:
        #         views.create_request_log(db, request.requestId, '', validate_response, 'response')
        #         return validate_response
        # else:
        #     response_hash = utils.create_ledger_hash(
        #         cache_response,
        #         validate_response.get('merchant_details').merchant_secret
        #     )
        #     cache_response.update({"signature": response_hash})
        #     return cache_response
    else:
        return api_request_log_res


@router.post("/status-check/{merchant_key}")
def check_whether_ledger_funded(merchant_key: str, request: CheckStatusSchema,
                                current_user: Annotated[User, Depends(get_current_merchant_active_user)],
                                db: Session = Depends(get_db)):
    logger.info(f"Current Merchant {current_user}")

    api_request_log_res = views.create_request_log(db, request.requestId, jsonable_encoder(request), '', 'request')
    if api_request_log_res.get("code") == 200:
        validate_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
        if validate_response.get('code') == 200:
            return_response = views.StatusCheck.ledger_status(
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


# @router.post("/cancel/{merchant_key}",
#              description=TagsMetadata.cancel_code.get('description'))
# def cancel_fund_ledger(merchant_key: str, request: CancelLedgerSchema,db: Session = Depends(get_db)):
#     api_request_log_res = views.create_request_log(db, request.requestId, jsonable_encoder(request), '', 'request')
#     if api_request_log_res.get("code") == 200:
#         validate_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
#         if validate_response.get('code') == 200:
#             return_response = views.CancelLedger.cancel(
#                 db,
#                 request,
#                 validate_response.get('merchant_details')
#             )
#             views.create_request_log(db, request.requestId, '', return_response, 'response')
#             return return_response
#         else:
#             views.create_request_log(db, request.requestId, '', validate_response, 'response')
#             return validate_response
#     else:
#         return api_request_log_res


@router.post("/get-webhook-data/{merchant_key}")
def get_webhook_data(merchant_key: str, request: CheckStatusSchema, db: Session = Depends(get_db)):
    return_response = views.webhook_data(db, request)
    return return_response
    # api_request_log_res = views.create_request_log(db, request.requestId, jsonable_encoder(request), '', 'request')
    # if api_request_log_res.get("code") == 200:
    #     validate_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
    #     if validate_response.get('code') == 200:
    #         return_response = views.webhook_data(db, request)
    #         views.create_request_log(db, request.requestId, '', return_response, 'response')
    #         return return_response
    #     else:
    #         views.create_request_log(db, request.requestId, '', validate_response, 'response')
    #         return validate_response
    # else:
    #     return api_request_log_res


@router.post("/get-signature/{merchant_key}")
def get_signature(merchant_key: str, request: dict, db: Session = Depends(get_db)):
    logger.info(request)
    merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
    logger.info(f"getting merchant details {merchant_details}")
    if merchant_details:
        created_signature = utils.create_signature(jsonable_encoder(request), merchant_details.merchant_secret)
        logger.info(f"created signature @@@@@@@@@@@@@@@@@@ {created_signature}")
        return {**ErrorCodes.get_error_response(200), "signature": created_signature}
    else:
        return {**ErrorCodes.get_error_response(1002)}


def get_cache(request):
    logger.info(f"getting request >>>>>>>>>>>>>> {request}")
    invoices_list = [sub['invoiceNo'] for sub in request]
    invoices_cache = r.get('invoices')
    logger.info(f"getting cache invoices no >>>>>>>>>>>>>>>. {invoices_cache}")
    if invoices_cache:
        check = any(item in invoices_list for item in ast.literal_eval(invoices_cache))
        if check:
            return {**ErrorCodes.get_error_response(1004)}
        else:
            new_cache_list = invoices_list + ast.literal_eval(invoices_cache)
            r.set('invoices', str(new_cache_list))
            # caches.remove('invoices')
            # cache['invoices'] = new_cache_list
            # caches.set('invoices', new_cache_list)
    else:
        logger.info(f"getting inside set invoice caches >>>>>>>>>>>. ")
        r.set('invoices', str(invoices_list))
        # cache['invoices'] = invoices_list
        # caches.set('invoices', invoices_list)

    invoice_cache = r.get('invoices')
    logger.info(f"final invoice caches>>>>>>>>>>>. {invoice_cache}")
    return {**ErrorCodes.get_error_response(200), "invoicesList": invoices_list}


@router.post("/get_cache/")
def get_cache_api(request: dict):
    logger.info(f"getting request >>>>>>>>>>>>>> {request}")
    invoices_list = [sub['invoiceNo'] for sub in request.get('ledgerData')]
    invoices_cache = r.get('invoices')
    logger.info(f"getting cache invoices no >>>>>>>>>>>>>>>. {invoices_cache}")
    if invoices_cache:
        check = any(item in invoices_list for item in ast.literal_eval(invoices_cache))
        if check:
            return {**ErrorCodes.get_error_response(1004)}
        # else:
        #     new_cache_list = invoices_list + ast.literal_eval(invoices_cache)
        # r.set('invoices', str(new_cache_list))
    # else:
    #     logger.info(f"getting inside set invoice caches >>>>>>>>>>>. ")
    #     r.set('invoices', str(invoices_list))

    # invoice_cache = r.get('invoices')
    # logger.info(f"final invoice caches>>>>>>>>>>>. {invoice_cache}")
    return {**ErrorCodes.get_error_response(200)}


@router.post("/read-csv-api/{merchant_key}")
def read_csv(merchant_key: str, db: Session = Depends(get_db)):
    from routers.registration import sync_registration_without_code
    from routers.finance import sync_finance
    import pysftp
    import paramiko
    import os
    try:
        registration_response = utils.read_csv_reg_finance()
        logger.info(f"getting registration_response data {registration_response}")
        with pysftp.Connection(host='34.100.208.190', username='test', password='Admin@123') as sftp:
            with sftp.cd('/home/test/django/'):  # temporarily chdir to public
                sftp.get('/home/test/django/ACCDemo/input/Invoice_Reg_Without_EC_Financing.csv',
                         '/home/django/incoming/Invoice_Reg_Without_EC_Financing.csv')
                registration_response = utils.read_csv_reg_finance()
                logger.info(f"getting registration_response data {registration_response}")
                response_csv_data = []
                if not registration_response[0].get('code', ''):
                    for reg_request_packet in registration_response:
                        merchant_details = db.query(MerchantDetails).filter(
                            MerchantDetails.unique_id == reg_request_packet.get('idpId')).first()
                        response_data = {
                            "channel": reg_request_packet.get('channel'),
                            "hubId": reg_request_packet.get('hubId'),
                            "idpId": reg_request_packet.get('idpId'),
                            "requestID": reg_request_packet.get('requestId'),
                            "groupingId": reg_request_packet.get('groupingId'),
                            "APIName": "Invoice Reg Without EC",
                            "ledgerNo": "",
                            "code": "",
                            "message": "",
                            "invoiceNo": "",
                            "invoiceDate": "",
                            "invoiceAmt": "",
                            "invoiceStatus": "",
                            "fundedAmt": "",
                            "gstVerificationStatus": ""
                        }
                        finance_data = reg_request_packet.get('financeData')
                        reg_request_packet.pop('channel')
                        reg_request_packet.pop('hubId')
                        reg_request_packet.pop('idpId')
                        reg_request_packet.pop('financeData')
                        logger.info(f"getting registration request packet {reg_request_packet}")
                        reg_signature = utils.create_signature(reg_request_packet, merchant_details.merchant_secret)
                        reg_request_packet.update({"signature": reg_signature})
                        try:
                            pay_load_json = InvoiceRequestSchema(**reg_request_packet)
                            pay_load = jsonable_encoder(pay_load_json)
                        except HTTPException as httpExc:
                            logger.info(f"httpExc:: {httpExc.detail}")
                            pay_load = {"code": 400, "error": str(httpExc.detail)}
                        except Exception as e:
                            logger.info(f" Exception:: {e}")
                            pay_load = {"code": 500, "error": str(e)}
                        logger.info(f"getting invoice registration schema response >>>>>>>>>>>> {pay_load}")
                        if not pay_load.get('error', ''):
                            reg_json_response = sync_registration_without_code(merchant_details.merchant_key, pay_load,
                                                                               db)
                            logger.info(f"getting registration response >>>>>>>>>>> {reg_json_response}")
                            response_data.update({
                                "APIName": "Invoice Reg Without EC",
                                "code": reg_json_response.get('code'),
                                "message": reg_json_response.get('message'),
                                "ledgerNo": reg_json_response.get('ledgerNo', ''),
                            })
                            response_csv_data.append(response_data)
                            if reg_json_response.get('code') == 200:
                                logger.info(f"getting inside finance >>>>>>>>>>> {finance_data}")
                                finance_response_data = copy.deepcopy(response_data)
                                for finance_request_packet in finance_data:
                                    logger.info(f"getting finance request packet >>>>>>>>>>>> {finance_request_packet}")
                                    finance_request_packet.update({"ledgerNo": reg_json_response.get('ledgerNo')})
                                    finance_signature = utils.create_signature(finance_request_packet,
                                                                               merchant_details.merchant_secret)
                                    finance_request_packet.update({"signature": finance_signature})

                                    try:
                                        finance_pay_load_json = AsyncFinanceSchema(**finance_request_packet)
                                        pay_load = jsonable_encoder(finance_pay_load_json)
                                    except HTTPException as httpExc:
                                        logger.info(f"httpExc:: {httpExc.detail}")
                                        pay_load = {"error": str(httpExc.detail)}
                                    except Exception as e:
                                        logger.info(f" Exception:: {e}")
                                        pay_load = {"error": str(e)}

                                    logger.info(f"getting finance schema response >>>>>>>>>>>> {pay_load}")
                                    if not pay_load.get('error', ''):
                                        finance_json_response = sync_finance(merchant_details.merchant_key, pay_load,
                                                                             db)
                                        logger.info(f"getting finance response >>>>>>>>>>>> {finance_json_response}")
                                        if finance_json_response.get('ledgerData'):
                                            for finance_response in finance_json_response.get('ledgerData'):
                                                finance_response_data = {
                                                    "channel": response_data.get('channel'),
                                                    "hubId": response_data.get('hubId'),
                                                    "idpId": response_data.get('idpId'),
                                                    "requestID": reg_request_packet.get('requestId'),
                                                    "groupingId": reg_request_packet.get('groupingId'),
                                                    "APIName": "Financing",
                                                    "ledgerNo": reg_json_response.get('ledgerNo'),
                                                    "code": finance_json_response.get('code'),
                                                    "message": finance_json_response.get('message'),
                                                    "invoiceNo": finance_response.get('invoiceNo'),
                                                    "invoiceDate": finance_response.get('invoiceDate'),
                                                    "invoiceAmt": finance_response.get('invoiceAmt'),
                                                    "invoiceStatus": finance_response.get('invoiceStatus'),
                                                    "fundedAmt": finance_response.get('fundedAmt'),
                                                    "gstVerificationStatus": finance_response.get('gstVerificationStatus')
                                                }
                                                response_csv_data.append(finance_response_data)
                                        else:
                                            finance_response_data.update(
                                                {
                                                    "code": finance_json_response.get('code'),
                                                    "APIName": "Financing",
                                                    "message": finance_json_response.get('message')
                                                }
                                            )
                                            response_csv_data.append(finance_response_data)

                                    else:
                                        logger.info(
                                            f"getting error of finance schema ########## {pay_load}")
                                        finance_response_data.update(
                                            {"code": "", "APIName": "Financing", "message": pay_load.get('error')})
                                        response_csv_data.append(finance_response_data)

                        else:
                            logger.info(f"getting error of registration schema ########## {pay_load}")
                            response_data.update({"code": "", "message": pay_load.get('error')})
                            response_csv_data.append(response_data)

                        # logger.info(f"getting registration json response {response_data} and {response_csv_data}")
                else:
                    logger.info(f"getting error of csv reading ########## {registration_response}")
                    response_data = {
                        "channel": '',
                        "hubId": '',
                        "idpId": '',
                        "requestID": '',
                        "groupingId": '',
                        "APIName": "Invoice Reg Without EC",
                        "ledgerNo": "",
                        "code": registration_response[0].get('code', ''),
                        "message": registration_response[0].get('message', ''),
                        "invoiceNo": "",
                        "invoiceDate": "",
                        "invoiceAmt": "",
                        "invoiceStatus": "",
                        "fundedAmt": "",
                        "gstVerificationStatus": ""
                    }
                    response_csv_data.append(response_data)

                field_name = [
                    "channel",
                    "hubId",
                    'idpId',
                    "requestID",
                    "groupingId",
                    "APIName",
                    "ledgerNo",
                    "code",
                    "message",
                    "invoiceNo",
                    "invoiceDate",
                    "invoiceAmt",
                    "invoiceStatus",
                    "fundedAmt",
                    "gstVerificationStatus"
                ]
                csv_response = utils.create_csv_response_file(
                    field_name,
                    '/home/django/outgoing/error_res.csv',
                    response_csv_data
                )
                sftp.rename('/home/test/django/ACCDemo/input/Invoice_Reg_Without_EC_Financing.csv',
                            '/home/test/django/ACCDemo/processed/Invoice_Reg_Without_EC_Financing.csv')
                sftp.put('/home/django/outgoing/error_res.csv',
                         '/home/test/django/ACCDemo/output/error_res.csv')  # upload file to public/ on remote
                # sftp.remove('/home/django/incoming/Invoice_Reg_Without_EC_Financing.csv')
                os.remove('/home/django/incoming/Invoice_Reg_Without_EC_Financing.csv')
                return csv_response

    except Exception as e:
        logger.info("Exception occurs while download error files from xms server {}".format(e))
        return False



###3)disbursement SFTP
@router.post("/read-csv-api-disburse/{merchant_key}")
def read_csv_disbursement(merchant_key: str, db: Session = Depends(get_db)):
    from routers.finance import sync_disbursement
    logger.info(f"getting merchant_key >>>>>>>>>>>>>> {merchant_key}")
    merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
    disbursement_response = utils.dis_read_csv_file()
    response_csv_data = []
    logger.info(f"disbursement_response<<<<<<->>>>>>>>>>>{disbursement_response}")
    for disb_request_packet in disbursement_response:
        logger.info(f"getting registration request packet {disb_request_packet}")
        response_data = {
            "channel": disb_request_packet.get('channel'),
            "hubId": disb_request_packet.get('hub'),
            "idpId": disb_request_packet.get('idpId'),
            "requestID": disb_request_packet.get('requestID'),
            "ledgerNo": "",
            "code": "",
            "message": ""
        }
        # channel = disb_request_packet.get('channel')
        # logger.info(f"getting Disbursement request packet {disb_request_packet}")
        disb_request_packet_copy = copy.deepcopy(disb_request_packet)
        disb_request_packet_copy.pop("channel")
        disb_request_packet_copy.pop("hubId")
        disb_request_packet_copy.pop("idpId")
        dis_signature = utils.create_signature(disb_request_packet_copy, merchant_details.merchant_secret)
        disb_request_packet.update({"signature": dis_signature})

        logger.info(f"getting Disbursement request packet {disb_request_packet}")
        try:
            dis_pay_load = AsyncDisburseSchema(**disb_request_packet)
            # pay_load = jsonable_encoder(pay_load_json)
        except HTTPException as httpExc:
            logger.info(f"httpExc:: {httpExc.detail}")
            dis_pay_load = {"code": 400, "error": str(httpExc.detail)}
        except Exception as e:
            logger.info(f" Exception:: {e}")
            dis_pay_load = {"code": 500, "error": str(e)}

        # if not pay_load.get('error', ''):
        #     disb_json_response = sync_disbursement(merchant_key, pay_load, db)
        #     response_data.update({
        #         "code": disb_json_response.get('code'),
        #         "message": disb_json_response.get('message')
        #     })
        #     response_csv_data.append(response_data)
        # else:
        #     response_data.update({"code": "", "message": pay_load.get('error')})
        #     response_csv_data.append(response_data)
        if isinstance(dis_pay_load, dict) and dis_pay_load.get('error'):
            response_data.update({"code": dis_pay_load.get('code'), "message": dis_pay_load.get('error')})
            response_csv_data.append(response_data)
            continue

        disb_api_response = sync_disbursement(merchant_key, dis_pay_load, db)
        logger.info(f"disbursement response:: {disb_api_response}")
        response_data.update({
            "code": disb_api_response.get('code'),
            "message": disb_api_response.get('message')
        })
        response_csv_data.append(response_data)

        # logger.info(f"getting disbursementtttttttttttt json response {response_data} and {response_csv_data}")
    field_name = [
        "channel",
        "hubId",
        "idpId",
        "requestID",
        "ledgerNo",
        "code",
        "message"
    ]
    csv_data = io.StringIO()
    csv_writer = csv.DictWriter(csv_data, fieldnames=field_name, delimiter="|")
    csv_writer.writeheader()
    csv_writer.writerows(response_csv_data)

    # This code for create file and write the csv data
    file_path = '/home/lalita/Documents/A1SFTP/error_disb_res.csv'
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(csv_data.getvalue())

    # Get the CSV string and encode it to bytes
    csv_bytes = csv_data.getvalue().encode("utf-8")

    # Set headers for download
    response = Response(content=csv_bytes)
    response.headers["Content-Disposition"] = 'attachment; filename="data.csv"'
    response.headers["Content-Type"] = "text/csv"
    return response


####4)repayment SFTP
@router.post("/read-csv-api-repayment/{merchant_key}")
def read_csv_repayment(merchant_key: str, db: Session = Depends(get_db)):
    from routers.finance import sync_repayment
    logger.info(f"getting merchant_key >>>>>>>>>>>>>> {merchant_key}")
    merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
    repayment_response = utils.repay_read_csv_file()
    # api_url = 'http://127.0.0.1:8000/syncDisbursement/bY8ziYU7OnHCLkrQRJdWPA'
    response_csv_data = []
    logger.info(f"repayment_response<<<<<<->>>>>>>>>>>{repayment_response}")
    for repay_request_packet in repayment_response:
        logger.info(f"getting registration request packet {repay_request_packet}")
        response_data = {
            "channel": repay_request_packet.get('channel'),
            "hubId": repay_request_packet.get('hub'),
            "idpId": repay_request_packet.get('idpId'),
            "requestID": repay_request_packet.get('requestID'),
            "ledgerNo": "",
            "code": "",
            "message": ""
        }
        # channel = repay_request_packet.get('channel')
        # hub_id = repay_request_packet.get('hubId')
        # idp_id = repay_request_packet.get('idpId')
        logger.info(f"getting repayment request packet {repay_request_packet}")
        repay_request_packet_copy = copy.deepcopy(repay_request_packet)
        repay_request_packet_copy.pop("channel")
        repay_request_packet_copy.pop("hubId")
        repay_request_packet_copy.pop("idpId")
        repay_signature = utils.create_signature(repay_request_packet_copy, merchant_details.merchant_secret)
        repay_request_packet.update({"signature": repay_signature})
        # repay_response = requests.post(api_url, json=repay_request_packet)
        # repay_json_response = json.loads(repay_response.text)
        try:
            repay_load = AsyncRepaymentSchema(**repay_request_packet)
            # pay_load = jsonable_encoder(pay_load_json)
        except HTTPException as httpExc:
            logger.info(f"httpExc:: {httpExc.detail}")
            repay_load = {"code": 400, "error": str(httpExc.detail)}
        except Exception as e:
            logger.info(f" Exception:: {e}")
            repay_load = {"code": 500, "error": str(e)}

        if isinstance(repay_load, dict) and repay_load.get('error'):
            response_data.update({"code": repay_load.get('code'), "message": repay_load.get('error')})
            response_csv_data.append(response_data)
            continue

        repay_api_response = sync_repayment(merchant_key, repay_load, db)
        logger.info(f"repayment response:: {repay_api_response}")
        response_data.update({
            "code": repay_api_response.get('code'),
            "message": repay_api_response.get('message')
        })
        response_csv_data.append(response_data)

        # logger.info(f"getting repayment json response {response_data} and {response_csv_data}")
    field_name = [
        "channel",
        "hubId",
        "idpId",
        "requestID",
        "ledgerNo",
        "code",
        "message"
    ]
    csv_data = io.StringIO()
    csv_writer = csv.DictWriter(csv_data, fieldnames=field_name, delimiter="|")
    csv_writer.writeheader()
    csv_writer.writerows(response_csv_data)

    # This code for create file and write the csv data
    file_path = '/home/lalita/Documents/A1SFTP/error_repay_res.csv'
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(csv_data.getvalue())

    # Get the CSV string and encode it to bytes
    csv_bytes = csv_data.getvalue().encode("utf-8")

    # Set headers for download
    response = Response(content=csv_bytes)
    response.headers["Content-Disposition"] = 'attachment; filename="data.csv"'
    response.headers["Content-Type"] = "text/csv"
    return response


@router.post("/read-csv-api-reg-without-code/{merchant_key}")
def read_csv_reg_without_ec(merchant_key: str, db: Session = Depends(get_db)):
    from routers.registration import sync_registration_without_code
    merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
    registration_response = utils.read_csv_reg_without_ec()
    response_csv_data = []
    for reg_request_packet in registration_response:
        response_data = {
            "channel": reg_request_packet.get('channel'),
            "hubId": reg_request_packet.get('hubId'),
            "idpId": reg_request_packet.get('idpId'),
            "requestID": reg_request_packet.get('requestId'),
            "groupingId": reg_request_packet.get('groupingId'),
            "APIName": "Invoice Reg Without EC",
            "ledgerNo": "",
            "code": "",
            "message": "",
            "invoiceNo": "",
            "invoiceDate": "",
            "invoiceAmt": "",
            "invoiceStatus": "",
            "fundedAmt": "",
            "gstVerificationStatus": ""
        }
        reg_request_packet.pop('channel')
        reg_request_packet.pop('hubId')
        reg_request_packet.pop('idpId')
        reg_request_packet.pop('financeData')
        logger.info(f"getting registration request packet {reg_request_packet}")
        reg_signature = utils.create_signature(reg_request_packet, merchant_details.merchant_secret)
        reg_request_packet.update({"signature": reg_signature})
        try:
            pay_load_json = InvoiceRequestSchema(**reg_request_packet)
            pay_load = jsonable_encoder(pay_load_json)
        except HTTPException as httpExc:
            logger.info(f"httpExc:: {httpExc.detail}")
            pay_load = {"code": 400, "error": str(httpExc.detail)}
        except Exception as e:
            logger.info(f" Exception:: {e}")
            pay_load = {"code": 500, "error": str(e)}

        if not pay_load.get('error', ''):
            reg_json_response = sync_registration_without_code(merchant_key, pay_load, db)
            response_data.update({
                "APIName": "Invoice Reg Without EC",
                "code": reg_json_response.get('code'),
                "message": reg_json_response.get('message')
            })
            response_csv_data.append(response_data)
        else:
            response_data.update({"code": "", "message": pay_load.get('error')})
            response_csv_data.append(response_data)

    field_name = [
        "channel",
        "hubId",
        'idpId',
        "requestID",
        "groupingId",
        "APIName",
        "ledgerNo",
        "code",
        "message",
        "invoiceNo",
        "invoiceDate",
        "invoiceAmt",
        "invoiceStatus",
        "fundedAmt",
        "gstVerificationStatus"
    ]
    csv_response = utils.create_csv_response_file(
        field_name,
        '/home/akhilesh/Documents/error_res.csv',
        response_csv_data
    )
    return csv_response


@router.post("/read-csv-api-entity-reg/{merchant_key}")
def read_csv_entity_reg(merchant_key: str, db: Session = Depends(get_db)):
    from routers.registration import sync_entity_registration
    merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
    entity_registration_response = utils.read_csv_entity_reg()
    response_csv_data = []
    for entity_reg_request_packet in entity_registration_response:
        response_data = {
            "channel": entity_reg_request_packet.get('channel'),
            "hubId": entity_reg_request_packet.get('hubId'),
            "idpId": entity_reg_request_packet.get('idpId'),
            "requestID": entity_reg_request_packet.get('requestId'),
            "code": "",
            "message": ""
        }
        entity_reg_request_packet.pop('channel')
        entity_reg_request_packet.pop('hubId')
        entity_reg_request_packet.pop('idpId')
        logger.info(f"getting registration request packet {entity_reg_request_packet}")
        entity_reg_signature = utils.create_signature(entity_reg_request_packet, merchant_details.merchant_secret)
        entity_reg_request_packet.update({"signature": entity_reg_signature})
        try:
            pay_load_json = EntityRegistrationSchema(**entity_reg_request_packet)
            pay_load = jsonable_encoder(pay_load_json)
        except HTTPException as httpExc:
            logger.info(f"httpExc:: {httpExc.detail}")
            pay_load = {"code": 400, "error": str(httpExc.detail)}
        except Exception as e:
            logger.info(f" Exception:: {e}")
            pay_load = {"code": 500, "error": str(e)}

        if not pay_load.get('error', ''):
            reg_json_response = sync_entity_registration(merchant_key, pay_load, db)
            response_data.update({
                "code": reg_json_response.get('code'),
                "message": reg_json_response.get('message')
            })
            response_csv_data.append(response_data)
        else:
            response_data.update({"code": "", "message": pay_load.get('error')})
            response_csv_data.append(response_data)

    field_name = [
        "channel",
        "hubId",
        'idpId',
        "requestID",
        "code",
        "message"
    ]
    csv_response = utils.create_csv_response_file(
        field_name,
        '/home/akhilesh/Documents/error_res.csv',
        response_csv_data
    )
    return csv_response


@router.post("/read-csv-api-reg-with-ec/{merchant_key}")
def read_csv_reg_with_ec(merchant_key: str, db: Session = Depends(get_db)):
    from routers.registration import sync_registration_without_code
    # merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
    registration_response = utils.read_csv_reg_without_ec()
    response_csv_data = []
    for reg_request_packet in registration_response:
        merchant_details = db.query(MerchantDetails).filter(
            MerchantDetails.id == reg_request_packet.get('idpId')).first()
        response_data = {
            "channel": reg_request_packet.get('channel'),
            "hubId": reg_request_packet.get('hubId'),
            "idpId": reg_request_packet.get('idpId'),
            "requestID": reg_request_packet.get('requestId'),
            "groupingId": reg_request_packet.get('groupingId'),
            "APIName": "Invoice Reg Without EC",
            "ledgerNo": "",
            "code": "",
            "message": "",
            "invoiceNo": "",
            "invoiceDate": "",
            "invoiceAmt": "",
            "invoiceStatus": "",
            "fundedAmt": "",
            "gstVerificationStatus": ""
        }
        reg_request_packet.pop('channel')
        reg_request_packet.pop('hubId')
        reg_request_packet.pop('idpId')
        reg_request_packet.pop('financeData')
        logger.info(f"getting registration request packet {reg_request_packet}")
        reg_signature = utils.create_signature(reg_request_packet, merchant_details.merchant_secret)
        reg_request_packet.update({"signature": reg_signature})
        try:
            pay_load_json = InvoiceRequestSchema(**reg_request_packet)
            pay_load = jsonable_encoder(pay_load_json)
        except HTTPException as httpExc:
            logger.info(f"httpExc:: {httpExc.detail}")
            pay_load = {"code": 400, "error": str(httpExc.detail)}
        except Exception as e:
            logger.info(f" Exception:: {e}")
            pay_load = {"code": 500, "error": str(e)}

        if not pay_load.get('error', ''):
            reg_json_response = sync_registration_without_code(merchant_key, pay_load, db)
            response_data.update({
                "APIName": "Invoice Reg Without EC",
                "code": reg_json_response.get('code'),
                "message": reg_json_response.get('message')
            })
            response_csv_data.append(response_data)
        else:
            response_data.update({"code": "", "message": pay_load.get('error')})
            response_csv_data.append(response_data)

    field_name = [
        "channel",
        "hubId",
        'idpId',
        "requestID",
        "groupingId",
        "APIName",
        "ledgerNo",
        "code",
        "message",
        "invoiceNo",
        "invoiceDate",
        "invoiceAmt",
        "invoiceStatus",
        "fundedAmt",
        "gstVerificationStatus"
    ]
    csv_response = utils.create_csv_response_file(
        field_name,
        '/home/akhilesh/Documents/error_res.csv',
        response_csv_data
    )
    return csv_response
