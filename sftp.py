import os
import copy
import logging
import time

import redis
import ast
import io
import requests
import json
import csv
import pytz
import re
import asyncio

import datetime as dt
import pandas as pd
import numpy as np

from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.encoders import jsonable_encoder
from decouple import config as dconfig

import models
import utils
from database import SessionLocal
from sqlalchemy.orm import Session
from models import MerchantDetails, Hub, SFTPUserInfo
from routers.auth import get_current_merchant_active_user, User
from routers.enquiry import sync_validation_service_with_code, sync_validation_service_without_code
from routers.sftp_headers_config import SFTP_API_HEADERS

from schema import InvoiceRequestSchema, FinanceSchema, CancelLedgerSchema, CheckStatusSchema, AsyncFinanceSchema, \
    AsyncDisburseSchema, AsyncRepaymentSchema, EntityRegistrationSchema, AsyncInvoiceRegistrationWithCodeSchema, \
    AsyncBulkFinanceSchema
import views
from errors import ErrorCodes
from utils import get_financial_year, check_invoice_date, create_csv_response_file
from database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)
__SFTP_HOST = str(dconfig('SFTP_HOST'))
__SFTP_USERNAME = str(dconfig('SFTP_USERNAME'))
__SFTP_PASSWORD = str(dconfig('SFTP_PASSWORD'))
__SFTP_PORT = int(dconfig('SFTP_PORT'))
__ENV_NAME = str(dconfig('ENVIRONMENT'))
__SEND_EMAIL = dconfig('SEND_EMAIL', default=False, cast=bool)

special_char_pattern = re.compile(r'^[a-zA-Z0-9_]*$')


@router.post("/testSftp")
def test_sftp():
    try:
        logger.info(f"/testSftp")
        # file_path = "/home/kailash/Downloads/Invoice_Registry_Project_Doc/sftp_files/dev$$abc$invoice_reg_with_entity_code_finance_disbursement$0015$maryam$001$07032024$122000$3.csv"
        # file_path = "/home/kailash/Downloads/dev%%abc%validate_without_ec%0015%maryam%001%07032024%122000%3.csv"
        # df = pd.read_csv(file_path, delimiter="|", dtype=str, na_values='')
        # df = df.replace(np.nan, '')
        # col_list = df.columns.tolist()
        # resp = prepare_request_data_of_idp_financing(df)
        # resp = prepare_request_data_of_idp_disbursement_repayment(df)
        # resp = prepare_request_data_of_idp_with_ec_reg_fin_dis(df)
        # resp = prepare_request_data_of_idp_financing(df)
        # resp = prepare_request_data_of_idp_val_ser_with_code(df)
        # resp = prepare_request_data_of_idp_val_ser_without_code(df)
        # logger.info(f"read_csv_to_reg_f_d :: response {resp}")
        # return resp
        fetch_upload_file_to_sftp()
        return {
            **ErrorCodes.get_error_response(200)
        }

        # from routers.mis_report import MisReport
        # resp = MisReport.financing_api_view()
        # resp = MisReport.cancellation_api_view()
        # resp = MisReport.registration_api_view()
        # resp = MisReport.disbursement_api_view()
        # resp = MisReport.repayment_api_view()
        # resp = MisReport.status_check_api_view()
        # logger.info(f"MisReport.financing_api_view :: response {resp}")
    except Exception as e:
        logger.exception(f"Exception test_sftp :: {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }


class RegistrationWithOutECFinanceDisbursementVaiCSV:

    @staticmethod
    def read_csv_to_reg_without_code_fin_dis(df):
        seller_identifier_data_list = []
        buyer_identifier_data_list = []
        seller_identifier_rows = 0
        buyer_identifier_rows = 0
        total_row_invoices = 0

        reg_request_packet = []
        finance_request_packet = []
        disburse_request_packet = []

        reg_ledger_data_list = []
        registration_obj = {}

        finance_obj = {}
        finance_ledger_data_list = []

        disburse_obj = {}
        disburse_ledger_data_list = []

        error_response = {"code": "", "message": ""}
        expected_headers = SFTP_API_HEADERS.invoice_reg_without_entity_code_finance_disbursement
        actual_headers = df.columns.tolist()
        # extra_headers = set(actual_headers) - set(expected_headers)
        extra_headers = set(expected_headers) - set(actual_headers)
        if extra_headers:
            error_response.update(**ErrorCodes.get_error_response(1090))
            return [error_response]

        try:
            for index, row in df.iterrows():
                reg_ledger_datas = ""
                finance_ledger_datas = ""
                disburse_ledger_datas = ""
                # logger.info(f" row.noOfInvoices {row.noOfInvoices}")

                # initial registration_obj = {}, it means very first time reg obj prepare
                if not registration_obj:
                    registration_obj = {
                        "requestId": str(row.requestID).strip(),
                        "sellerGst": str(row.sellerGst).strip(),
                        "buyerGst": str(row.buyerGst).strip(),
                        "groupingId": str(row.groupingId).strip(),
                        "total_invoice": row.noOfInvoices,
                        "total_buyer_identifier_request": row.buyerDataNo if row.buyerDataNo else 0,
                        "total_buyer_identifier_count": 0,
                        "total_seller_identifier_request": row.sellerDataNo if row.sellerDataNo else 0,
                        "total_seller_identifier_count": 0
                    }
                    finance_obj = {
                        "requestId": str(row.requestID).strip(),
                        "ledgerAmtFlag": str(row.ledgerAmtFlag).strip(),
                        "lenderCategory": str(row.lenderCategory).strip(),
                        "lenderName": str(row.lenderName).strip(),
                        "lenderCode": str(row.lenderCode).strip(),
                        "borrowerCategory": str(row.borrowerCategory).strip(),
                    }
                    disburse_obj = {
                        "requestId": str(row.requestID).strip(),
                        "lenderCategory": str(row.lenderCategory).strip(),
                        "lenderName": str(row.lenderName).strip(),
                        "lenderCode": str(row.lenderCode).strip(),
                    }
                else:

                    if str(registration_obj.get('requestId')).strip() != str(row.requestID).strip():
                        reg_request_packet.append(registration_obj)
                        finance_request_packet.append(finance_obj)
                        disburse_request_packet.append(disburse_obj)

                        registration_obj = {
                            "requestId": str(row.requestID).strip(),
                            "sellerGst": str(row.sellerGst).strip(),
                            "buyerGst": str(row.buyerGst).strip(),
                            "groupingId": row.groupingId,
                            "total_invoice": row.noOfInvoices,
                            "total_buyer_identifier_request": row.buyerDataNo if row.buyerDataNo else 0,
                            "total_buyer_identifier_count": 0,
                            "total_seller_identifier_request": row.sellerDataNo if row.sellerDataNo else 0,
                            "total_seller_identifier_count": 0
                        }

                        finance_obj = {
                            "requestId": str(row.requestID).strip(),
                            "ledgerAmtFlag": str(row.ledgerAmtFlag).strip(),
                            "lenderCategory": str(row.lenderCategory).strip(),
                            "lenderName": str(row.lenderName).strip(),
                            "lenderCode": str(row.lenderCode).strip(),
                            "borrowerCategory": str(row.borrowerCategory).strip(),
                        }
                        disburse_obj = {
                            "requestId": str(row.requestID).strip(),
                            "lenderCategory": str(row.lenderCategory).strip(),
                            "lenderName": str(row.lenderName).strip(),
                            "lenderCode": str(row.lenderCode).strip(),
                        }

                        reg_ledger_data_list = []
                        finance_ledger_data_list = []
                        disburse_ledger_data_list = []

                        seller_identifier_data_list = []
                        buyer_identifier_data_list = []
                        total_row_invoices = 0
                        seller_identifier_rows = 0
                        buyer_identifier_rows = 0

                if index == df.shape[0] - 1 and str(registration_obj.get('requestId')) == str(row.requestID):
                    reg_request_packet.append(registration_obj)
                    finance_request_packet.append(finance_obj)
                    disburse_request_packet.append(disburse_obj)

                gst_status = str(row.verifyGSTNFlag).strip()
                if row.verifyGSTNFlag == str(1):
                    gst_status = True
                if row.verifyGSTNFlag == str(0):
                    gst_status = False

                if str(row.invoiceNo).strip() or str(row.invoiceDate).strip() or str(
                        row.invoiceAmt).strip() or gst_status or str(row.dueDate).strip():
                    reg_ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip(),
                        "verifyGSTNFlag": gst_status,
                        "invoiceDueDate": row.invoiceDueDate,
                    }
                    finance_ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "financeRequestAmt": str(row.financeRequestAmt).strip(),
                        "financeRequestDate": str(row.financeRequestDate).strip(),
                        "dueDate": str(row.dueDate).strip(),
                        "fundingAmtFlag": str(row.fundingAmtFlag).strip(),
                        "adjustmentType": str(row.adjustmentType).strip(),
                        "adjustmentAmt": str(row.adjustmentAmt).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip()
                    }
                    disburse_ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "disbursedFlag": str(row.disbursedFlag).strip(),
                        "disbursedAmt": str(row.disbursedAmt).strip(),
                        "disbursedDate": str(row.disbursedDate).strip(),
                        "dueAmt": str(row.dueAmt).strip(),
                        "dueDate": str(row.dueDate).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip()
                    }
                    total_row_invoices = total_row_invoices + 1

                if str(row.sellerIdType).strip() or str(row.sellerIdNo).strip() or str(row.sellerIdName).strip() or str(
                        row.sellerIfsc).strip():
                    seller_identifier_rows = seller_identifier_rows + 1
                    registration_obj.update({"total_seller_identifier_count": str(seller_identifier_rows)})

                seller_identifier_datas = {
                    "sellerIdType": str(row.sellerIdType).strip(),
                    "sellerIdNo": str(row.sellerIdNo).strip(),
                    "sellerIdName": str(row.sellerIdName).strip(),
                    "ifsc": str(row.sellerIfsc).strip() if str(row.sellerIfsc) else ""
                }

                if str(row.buyerIdType).strip() or str(row.buyerIdNo).strip() or str(row.buyerIdName).strip() or str(
                        row.buyerIfsc).strip():
                    buyer_identifier_rows = buyer_identifier_rows + 1
                    registration_obj.update({"total_buyer_identifier_count": str(buyer_identifier_rows)})

                buyer_identifier_datas = {
                    "buyerIdType": str(row.buyerIdType).strip(),
                    "buyerIdNo": str(row.buyerIdNo).strip(),
                    "buyerIdName": str(row.buyerIdName).strip(),
                    "ifsc": str(row.buyerIfsc).strip() if str(row.buyerIfsc) else ""
                }

                if reg_ledger_datas:
                    reg_ledger_data_list.append(reg_ledger_datas)
                    finance_ledger_data_list.append(finance_ledger_datas)
                    disburse_ledger_data_list.append(disburse_ledger_datas)

                if seller_identifier_datas:
                    seller_identifier_data_list.append(seller_identifier_datas)

                if buyer_identifier_datas:
                    buyer_identifier_data_list.append(buyer_identifier_datas)

                if reg_ledger_datas:
                    registration_obj.update({
                        "ledgerData": reg_ledger_data_list,
                        "sellerIdentifierData": seller_identifier_data_list,
                        "buyerIdentifierData": buyer_identifier_data_list,
                        "channel": str(row.channel).strip(),
                        "hubId": str(row.hubId).strip(),
                        "idpId": str(row.idpId).strip()
                    })

                    finance_obj.update({"ledgerData": finance_ledger_data_list})
                    disburse_obj.update({"ledgerData": disburse_ledger_data_list})

            logger.info(f"after csv reading :: read_csv_to_reg_without_code_fin_dis :: ............")
            logger.info(f"Reg request packet {reg_request_packet}")
            logger.info(f"Fin request packet {finance_request_packet}")
            logger.info(f"Dis request packet {disburse_request_packet}")
            request_packet = [{
                "registration_request_packet": reg_request_packet or [{}],
                "finance_request_packet": finance_request_packet or [{}],
                "disburse_request_packet": disburse_request_packet or [{}]
            }]
            return request_packet
        except Exception as e:
            logger.exception(f"Exception read_csv_to_reg_without_code_fin_dis:: {e}")
            return [{"code": 500, "message": str(e)}]

    @staticmethod
    def do_reg_without_code_fin_disb(request_packets):
        db = next(get_db())
        from routers.registration import sync_registration_without_code
        from routers.finance import sync_finance, sync_disbursement
        try:
            logger.info(f"do_reg_without_code_fin_disb ::.............")
            response_csv_data = []
            request_packets = request_packets[0]
            if request_packets.get('code', ''):
                response_data = {
                    "channel": '',
                    "hubId": '',
                    "idpId": '',
                    "requestID": '',
                    "groupingId": '',
                    "APIName": "Invoice Reg Without EC",
                    "ledgerNo": "",
                    "code": request_packets.get('code', ''),
                    "message": request_packets.get('message', ''),
                    "invoiceNo": "",
                    "invoiceDate": "",
                    "invoiceAmt": "",
                    "invoiceStatus": "",
                    "fundedAmt": "",
                    "gstVerificationStatus": ""
                }
                response_csv_data.append(response_data)
                logger.info(f"don't do_reg_without_code_fin_disb :: if payload have error :: {response_csv_data}")
                return response_csv_data

            registration_request_packets = request_packets.get("registration_request_packet")
            finance_request_packets = request_packets.get("finance_request_packet")
            disburse_request_packets = request_packets.get("disburse_request_packet")

            for reg_request_packet in registration_request_packets:
                logger.info(f"let's process:: Registration request packet :: ........")
                response_data = {
                    "channel": reg_request_packet.get('channel'),
                    "hubId": reg_request_packet.get('hubId'),
                    "idpId": reg_request_packet.get('idpId'),
                    "requestId": reg_request_packet.get('requestId'),
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
                mer_unique_id = reg_request_packet.get('idpId', '')
                merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()

                validate_column_response = validate_column(reg_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                merchant_key = merchant_details.merchant_key

                reg_request_packet.pop('total_buyer_identifier_request')
                reg_request_packet.pop('total_buyer_identifier_count')
                reg_request_packet.pop('total_seller_identifier_request')
                reg_request_packet.pop('total_seller_identifier_count')
                reg_request_packet.pop('total_invoice')

                reg_request_packet_copy = copy.deepcopy(reg_request_packet)
                reg_request_packet_copy.pop("channel")
                reg_request_packet_copy.pop("hubId")
                reg_request_packet_copy.pop("idpId")

                reg_signature = utils.create_signature(reg_request_packet_copy, merchant_details.merchant_secret)
                reg_request_packet.update({"signature": reg_signature})
                logger.info(f"getting registration request packet {reg_request_packet}")
                try:
                    reg_pay_load = InvoiceRequestSchema(**reg_request_packet)
                except HTTPException as httpExc:
                    logger.info(f"httpExc:: {httpExc.detail}")
                    reg_pay_load = {"code": 400, "error": str(httpExc.detail)}
                except Exception as e:
                    logger.info(f" Exception:: {e}")
                    reg_pay_load = {"code": 500, "error": str(e)}

                if isinstance(reg_pay_load, dict) and reg_pay_load.get('error'):
                    response_data.update({"code": reg_pay_load.get('code'), "message": reg_pay_load.get('error')})
                    response_csv_data.append(response_data)
                    continue

                reg_api_response = sync_registration_without_code(merchant_key, reg_pay_load, db)
                logger.info(f"After Registration, api response:: {reg_api_response}")
                api_request_obj = db.query(
                    models.APIRequestLog
                ).filter(
                    models.APIRequestLog.request_id == reg_request_packet.get('requestId')
                ).first()
                if api_request_obj:
                    data = copy.deepcopy(api_request_obj.extra_data)
                    data.update({
                        "channel": reg_request_packet.get('channel'),
                        "hubId": reg_request_packet.get('hubId'),
                        "idpId": reg_request_packet.get('idpId'),
                        "groupingId": reg_request_packet.get('groupingId')
                    })
                    api_request_obj.extra_data = data
                    db.commit()
                    db.refresh(api_request_obj)

                response_data.update({
                    "APIName": "Invoice Reg Without EC",
                    "code": reg_api_response.get('code'),
                    "message": reg_api_response.get('message'),
                    "ledgerNo": reg_api_response.get('ledgerNo', '')
                })
                response_csv_data.append(response_data)

                if reg_api_response.get('code') == 200:
                    # Financing
                    for finance_request_packet in finance_request_packets:
                        if finance_request_packet.get('requestId') == reg_request_packet.get('requestId'):
                            finance_response_data = {
                                "channel": reg_request_packet.get('channel'),
                                "hubId": reg_request_packet.get('hubId'),
                                "idpId": reg_request_packet.get('idpId'),
                                "requestId": reg_request_packet.get('requestId'),
                                "groupingId": reg_request_packet.get('groupingId'),
                                "APIName": "syncFinancing",
                                "ledgerNo": reg_api_response.get('ledgerNo'),
                                "code": "",
                                "message": "",
                                "invoiceNo": finance_request_packet.get('invoiceNo'),
                                "invoiceDate": finance_request_packet.get('invoiceDate'),
                                "invoiceAmt": finance_request_packet.get('invoiceAmt'),
                                "invoiceStatus": "",
                                "fundedAmt": "",
                                "gstVerificationStatus": ""
                            }

                            # finance_request_packet.update({"ledgerNo": reg_api_response.get('ledgerNo')})
                            fin_req_id = 'finance_' + finance_request_packet.get('requestId')
                            items = list(finance_request_packet.items())
                            items.insert(1, ("requestId", fin_req_id))
                            items.insert(2, ("ledgerNo", reg_api_response.get('ledgerNo')))
                            finance_request_packet = dict(items)

                            finance_signature = utils.create_signature(finance_request_packet,
                                                                       merchant_details.merchant_secret)
                            finance_request_packet.update({"signature": finance_signature})
                            logger.info(f"finance request packet {finance_request_packet}")
                            try:
                                fin_pay_load = AsyncBulkFinanceSchema(**finance_request_packet)
                            except HTTPException as httpExc:
                                logger.info(f"httpExc:: {httpExc.detail}")
                                fin_pay_load = {"code": 400, "error": str(httpExc.detail)}
                            except Exception as e:
                                logger.info(f" Exception:: {e}")
                                fin_pay_load = {"code": 500, "error": str(e)}

                            if isinstance(fin_pay_load, dict) and fin_pay_load.get('error', ''):
                                finance_response_data.update({
                                    "code": fin_pay_load.get('code'),
                                    "message": fin_pay_load.get('error')
                                })
                                response_csv_data.append(finance_response_data)
                                continue

                            finance_api_resp = sync_finance(merchant_key, fin_pay_load, db)
                            logger.info(f"finance_api_resp : {finance_api_resp}")

                            api_request_obj = db.query(
                                models.APIRequestLog
                            ).filter(
                                models.APIRequestLog.request_id == fin_req_id
                            ).first()
                            if api_request_obj:
                                data = copy.deepcopy(api_request_obj.extra_data)
                                data.update({
                                    "channel": reg_request_packet.get('channel'),
                                    "hubId": reg_request_packet.get('hubId'),
                                    "idpId": reg_request_packet.get('idpId'),
                                    "groupingId": reg_request_packet.get('groupingId')
                                })
                                api_request_obj.extra_data = data
                                db.commit()
                                db.refresh(api_request_obj)

                            # write finance api resp to csv
                            if finance_api_resp.get('ledgerData'):
                                for fin_ledger_data in finance_api_resp.get('ledgerData'):
                                    finance_response_data = {
                                        "channel": reg_request_packet.get('channel'),
                                        "hubId": reg_request_packet.get('hubId'),
                                        "idpId": reg_request_packet.get('idpId'),
                                        "requestId": reg_request_packet.get('requestId'),
                                        "groupingId": reg_request_packet.get('groupingId'),
                                        "APIName": "syncFinancing",
                                        "ledgerNo": reg_api_response.get('ledgerNo'),
                                        "code": finance_api_resp.get('code'),
                                        "message": finance_api_resp.get('message'),
                                        "invoiceNo": fin_ledger_data.get('invoiceNo'),
                                        "invoiceDate": fin_ledger_data.get('invoiceDate'),
                                        "invoiceAmt": fin_ledger_data.get('invoiceAmt'),
                                        "invoiceStatus": fin_ledger_data.get('invoiceStatus'),
                                        "fundedAmt": fin_ledger_data.get('fundedAmt'),
                                        "gstVerificationStatus": fin_ledger_data.get('gstVerificationStatus')
                                    }
                                    response_csv_data.append(finance_response_data)
                            else:
                                finance_response_data = {
                                    "channel": reg_request_packet.get('channel'),
                                    "hubId": reg_request_packet.get('hubId'),
                                    "idpId": reg_request_packet.get('idpId'),
                                    "requestId": reg_request_packet.get('requestId'),
                                    "groupingId": reg_request_packet.get('groupingId'),
                                    "APIName": "syncFinancing",
                                    "ledgerNo": reg_api_response.get('ledgerNo'),
                                    "code": finance_api_resp.get('code'),
                                    "message": finance_api_resp.get('message'),
                                    "invoiceNo": "",
                                    "invoiceDate": "",
                                    "invoiceAmt": "",
                                    "invoiceStatus": "",
                                    "fundedAmt": "",
                                    "gstVerificationStatus": ""
                                }
                                response_csv_data.append(finance_response_data)

                            # call disburse api if ledger is funded successfully
                            if finance_api_resp.get('code') in [1013]:
                                for disburse_request_packet in disburse_request_packets:
                                    if disburse_request_packet.get('requestId') == reg_request_packet.get('requestId'):
                                        dis_response_data = {
                                            "channel": reg_request_packet.get('channel'),
                                            "hubId": reg_request_packet.get('hubId'),
                                            "idpId": reg_request_packet.get('idpId'),
                                            "requestId": reg_request_packet.get('requestId'),
                                            "groupingId": reg_request_packet.get('groupingId'),
                                            "APIName": "syncDisbursement",
                                            "ledgerNo": reg_api_response.get('ledgerNo'),
                                            "code": "",
                                            "message": "",
                                            "invoiceNo": "",
                                            "invoiceDate": "",
                                            "invoiceAmt": "",
                                            "invoiceStatus": "",
                                            "fundedAmt": "",
                                            "gstVerificationStatus": ""
                                        }
                                        # disburse_request_packet.update({"ledgerNo": reg_api_response.get('ledgerNo')})
                                        dis_req_id = 'disburse_' + finance_request_packet.get('requestId')
                                        # disburse_request_packet.update({"requestId": dis_req_id})

                                        items = list(disburse_request_packet.items())
                                        items.insert(1, ("requestId", dis_req_id))
                                        items.insert(2, ("ledgerNo", reg_api_response.get('ledgerNo')))
                                        disburse_request_packet = dict(items)

                                        disburse_signature = utils.create_signature(disburse_request_packet,
                                                                                    merchant_details.merchant_secret)
                                        disburse_request_packet.update({"signature": disburse_signature})
                                        logger.info(f"disburse request packet {disburse_request_packet}")

                                        try:
                                            dis_pay_load = AsyncDisburseSchema(**disburse_request_packet)
                                        except HTTPException as httpExc:
                                            logger.info(f"httpExc:: {httpExc.detail}")
                                            dis_pay_load = {"code": 400, "error": str(httpExc.detail)}
                                        except Exception as e:
                                            logger.info(f" Exception:: {e}")
                                            dis_pay_load = {"code": 500, "error": str(e)}

                                        if isinstance(dis_pay_load, dict) and dis_pay_load.get('error', ''):
                                            dis_response_data.update({
                                                "code": dis_pay_load.get('code'),
                                                "message": dis_pay_load.get('error')
                                            })
                                            response_csv_data.append(dis_response_data)
                                            continue

                                        disburse_api_resp = sync_disbursement(merchant_key, dis_pay_load, db)
                                        dis_response_data.update({
                                            "code": disburse_api_resp.get('code'),
                                            "message": disburse_api_resp.get('message')
                                        })
                                        logger.info(f"After Reg,Fin,disburse api response::{disburse_request_packet}")
                                        api_request_obj = db.query(models.APIRequestLog).filter(
                                            models.APIRequestLog.request_id == dis_req_id
                                        ).first()
                                        if api_request_obj:
                                            data = copy.deepcopy(api_request_obj.extra_data)
                                            data.update({
                                                "channel": reg_request_packet.get('channel'),
                                                "hubId": reg_request_packet.get('hubId'),
                                                "idpId": reg_request_packet.get('idpId'),
                                                "groupingId": reg_request_packet.get('groupingId')
                                            })
                                            api_request_obj.extra_data = data
                                            db.commit()
                                            db.refresh(api_request_obj)
                                        response_csv_data.append(dis_response_data)
            return response_csv_data
        except Exception as e:
            logger.exception(f"Exception do_reg_without_code_fin_disb :: {e}")
            return {
                **ErrorCodes.get_error_response(500)
            }


class FinancingVaiCSV:

    @staticmethod
    def read_csv_to_financing(df):
        total_row_invoices = 0
        total_invoice_request_count = 0
        finance_request_packet = []
        finance_obj = {}
        finance_ledger_data_list = []
        finance_ledger_datas = []

        error_response = {"code": "", "message": ""}
        expected_headers = SFTP_API_HEADERS.finance

        actual_headers = df.columns.tolist()
        extra_headers = set(expected_headers) - set(actual_headers)
        # extra_headers = set(actual_headers) - set(expected_headers)
        if extra_headers:
            error_response.update(**ErrorCodes.get_error_response(1090))
            return [error_response]

        try:
            logger.info(f" read_csv_to_financing :: ............")
            for index, row in df.iterrows():
                logger.info(f" row.noOfInvoices {row.noOfInvoices}")

                if row.noOfInvoices:
                    # if total_row_invoices and total_row_invoices != total_invoice_request_count:
                    #     return [{
                    #         "requestId": str(row.requestID).strip(),
                    #         "code": 500,
                    #         "message": "invalid no of invoices count"
                    #     }]
                    row.noOfInvoices = utils.cast_to_int_or_zero(row.noOfInvoices)
                    # total_invoice_request_count = int(row.noOfInvoices)

                # initial finance_obj = {}, it means very first time reg obj prepare
                if not finance_obj:
                    finance_obj = {
                        "requestId": str(row.requestID).strip(),
                        "ledgerNo": str(row.ledgerNo).strip(),
                        "ledgerAmtFlag": str(row.ledgerAmtFlag).strip(),
                        "lenderCategory": str(row.lenderCategory).strip(),
                        "lenderName": str(row.lenderName).strip(),
                        "lenderCode": str(row.lenderCode).strip(),
                        "borrowerCategory": str(row.borrowerCategory).strip(),
                        "total_invoice": row.noOfInvoices,
                    }
                else:
                    if str(finance_obj.get('requestId')).strip() != str(row.requestID).strip():
                        finance_request_packet.append(finance_obj)

                        finance_obj = {
                            "requestId": str(row.requestID).strip(),
                            "ledgerNo": str(row.ledgerNo).strip(),
                            "ledgerAmtFlag": str(row.ledgerAmtFlag).strip(),
                            "lenderCategory": str(row.lenderCategory).strip(),
                            "lenderName": str(row.lenderName).strip(),
                            "lenderCode": str(row.lenderCode).strip(),
                            "borrowerCategory": str(row.borrowerCategory).strip(),
                            "total_invoice": row.noOfInvoices,
                        }
                        finance_ledger_data_list = []
                        total_row_invoices = 0
                        total_invoice_request_count = 0

                if index == df.shape[0] - 1 and str(finance_obj.get('requestId')).strip() == str(row.requestID).strip():
                    finance_request_packet.append(finance_obj)

                if str(row.invoiceNo).strip() or str(row.invoiceDate).strip() or str(row.invoiceAmt).strip() or str(
                        row.dueDate).strip():
                    finance_ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "financeRequestAmt": str(row.financeRequestAmt).strip(),
                        "financeRequestDate": str(row.financeRequestDate).strip(),
                        "dueDate": str(row.dueDate).strip(),
                        "fundingAmtFlag": str(row.fundingAmtFlag).strip(),
                        "adjustmentType": str(row.adjustmentType).strip(),
                        "adjustmentAmt": str(row.adjustmentAmt).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip()
                    }
                    total_row_invoices = total_row_invoices + 1

                if finance_ledger_datas:
                    finance_ledger_data_list.append(finance_ledger_datas)

                if finance_ledger_datas:
                    finance_obj.update({
                        "ledgerData": finance_ledger_data_list,
                        "channel": str(row.channel).strip(),
                        "hubId": str(row.hubId).strip(),
                        "idpId": str(row.idpId).strip()
                    })
                finance_obj.update({"ledgerData": finance_ledger_data_list})
            logger.info(f"read_csv_to_financing : response :: {finance_request_packet}")
            return finance_request_packet
        except Exception as e:
            logger.exception(f"Exception read_csv_to_financing :: {e}")
            # raise HTTPException(status_code=404, detail="Exception found")
            return [{"code": 500, "message": str(e)}]

    @staticmethod
    def do_financing(finance_request_payload):
        db = next(get_db())
        from routers.finance import sync_finance
        try:
            logger.info(f"do_financing :: ...........")
            response_csv_data = []
            if finance_request_payload[0].get("code", ''):
                response_data = {
                    "channel": "",
                    "hubId": "",
                    "idpId": "",
                    "requestId": "",
                    "ledgerNo": "",
                    "code": finance_request_payload[0].get("code", ''),
                    "message": finance_request_payload[0].get("message", ''),
                    "invoiceNo": "",
                    "invoiceDate": "",
                    "invoiceAmt": "",
                    "invoiceStatus": "",
                    "fundedAmt": "",
                    "gstVerificationStatus": ""
                }
                response_csv_data.append(response_data)
                logger.info(f"don't do_financing :: if payload have error :: {response_csv_data}")
                return response_csv_data

            for finance_request_packet in finance_request_payload:
                logger.info(f"let's process:: finance request packet :: ........")
                response_data = {
                    "channel": finance_request_packet.get('channel'),
                    "hubId": finance_request_packet.get('hubId'),
                    "idpId": finance_request_packet.get('idpId'),
                    "requestId": finance_request_packet.get('requestId'),
                    "ledgerNo": finance_request_packet.get('ledgerNo'),
                    "code": "",
                    "message": "",
                    "invoiceNo": "",
                    "invoiceDate": "",
                    "invoiceAmt": "",
                    "invoiceStatus": "",
                    "fundedAmt": "",
                    "gstVerificationStatus": ""
                }

                mer_unique_id = finance_request_packet.get('idpId', '')
                merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()

                validate_column_response = validate_column(finance_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                merchant_key = merchant_details.merchant_key

                finance_request_packet.pop("total_invoice")
                fin_request_packet_copy = copy.deepcopy(finance_request_packet)
                fin_request_packet_copy.pop("channel")
                fin_request_packet_copy.pop("hubId")
                fin_request_packet_copy.pop("idpId")

                fin_signature = utils.create_signature(fin_request_packet_copy, merchant_details.merchant_secret)
                finance_request_packet.update({"signature": fin_signature})

                logger.info(f"getting finance request packet :: {finance_request_packet}")
                try:
                    fin_pay_load = AsyncBulkFinanceSchema(**finance_request_packet)
                except HTTPException as httpExc:
                    logger.info(f"httpExc:: {httpExc.detail}")
                    fin_pay_load = {"code": 400, "error": str(httpExc.detail)}
                except Exception as e:
                    logger.info(f" Exception:: {e}")
                    fin_pay_load = {"code": 500, "error": str(e)}

                if isinstance(fin_pay_load, dict) and fin_pay_load.get('error'):
                    response_data.update({"code": fin_pay_load.get('code'), "message": fin_pay_load.get('error')})
                    response_csv_data.append(response_data)
                    continue

                finance_api_resp = sync_finance(merchant_key, fin_pay_load, db)
                logger.info(f"After financing, api response:: {finance_api_resp}")
                api_request_obj = db.query(
                    models.APIRequestLog
                ).filter(
                    models.APIRequestLog.request_id == finance_request_packet.get('requestId')
                ).first()
                if api_request_obj:
                    data = copy.deepcopy(api_request_obj.extra_data)
                    data.update({
                        "channel": finance_request_packet.get('channel'),
                        "hubId": finance_request_packet.get('hubId'),
                        "idpId": finance_request_packet.get('idpId')
                        # "groupingId": finance_request_packet.get('groupingId')
                    })
                    api_request_obj.extra_data = data
                    db.commit()
                    db.refresh(api_request_obj)

                # write finance api resp to csv
                if finance_api_resp.get('ledgerData'):
                    for fin_ledger_data in finance_api_resp.get('ledgerData'):
                        response_data = {
                            "channel": finance_request_packet.get('channel'),
                            "hubId": finance_request_packet.get('hubId'),
                            "idpId": finance_request_packet.get('idpId'),
                            "requestId": finance_request_packet.get('requestId'),
                            "ledgerNo": finance_request_packet.get('ledgerNo'),
                            "code": finance_api_resp.get('code'),
                            "message": finance_api_resp.get('message'),
                            "invoiceNo": fin_ledger_data.get('invoiceNo'),
                            "invoiceDate": fin_ledger_data.get('invoiceDate'),
                            "invoiceAmt": fin_ledger_data.get('invoiceAmt'),
                            "invoiceStatus": fin_ledger_data.get('invoiceStatus'),
                            "fundedAmt": fin_ledger_data.get('fundedAmt'),
                            "gstVerificationStatus": fin_ledger_data.get('gstVerificationStatus')
                        }
                        response_csv_data.append(response_data)
                else:
                    response_data = {
                        "channel": finance_request_packet.get('channel'),
                        "hubId": finance_request_packet.get('hubId'),
                        "idpId": finance_request_packet.get('idpId'),
                        "requestId": finance_request_packet.get('requestId'),
                        "ledgerNo": finance_request_packet.get('ledgerNo'),
                        "code": finance_api_resp.get('code'),
                        "message": finance_api_resp.get('message'),
                        "invoiceNo": finance_request_packet.get('invoiceNo'),
                        "invoiceDate": finance_request_packet.get('invoiceDate'),
                        "invoiceAmt": finance_request_packet.get('invoiceAmt'),
                        "invoiceStatus": "",
                        "fundedAmt": "",
                        "gstVerificationStatus": ""
                    }
                    response_csv_data.append(response_data)
            return response_csv_data
        except Exception as e:
            logger.exception(f"Exception do_financing :: {e}")
            return {
                **ErrorCodes.get_error_response(500)
            }


# READ CSV FOR DISDURSEMENT AND REPAYMENT
class DisbursementRepaymentVaiCSV:

    @staticmethod
    def read_csv_to_disbursement_repayment(df):
        total_row_invoices = 0
        total_invoice_request_count = 0

        disburse_request_packet = []
        repay_request_packet = []

        disburse_obj = {}
        disburse_ledger_data_list = []

        repayment_obj = {}
        repayment_ledger_data_list = []

        error_response = {"code": "", "message": ""}
        expected_headers = SFTP_API_HEADERS.disbursement_repayment

        actual_headers = df.columns.tolist()
        extra_headers = set(expected_headers) - set(actual_headers)
        # extra_headers = set(actual_headers) - set(expected_headers)
        if extra_headers:
            error_response.update(**ErrorCodes.get_error_response(1090))
            return [error_response]

        try:
            for index, row in df.iterrows():
                logger.info(f" row.noOfInvoices {row.noOfInvoices}")
                disburse_ledger_dict = {}
                repayment_ledger_dict = {}
                if row.noOfInvoices:
                    # if total_row_invoices and total_row_invoices != total_invoice_request_count:
                    #     return [{
                    #         "requestId": str(row.requestID).strip(),
                    #         "code": 500,
                    #         "message": "invalid no of invoices count"
                    #     }]
                    row.noOfInvoices = utils.cast_to_int_or_zero(row.noOfInvoices)
                    # total_invoice_request_count = int(row.noOfInvoices)

                # initial disburse_obj = {}, it means very first time dis obj prepare
                if not disburse_obj:
                    disburse_obj = {
                        "requestId": str(row.requestID).strip(),
                        "ledgerNo": str(row.ledgerNo).strip(),
                        "lenderCategory": str(row.lenderCategory).strip(),
                        "lenderName": str(row.lenderName).strip(),
                        "lenderCode": str(row.lenderCode).strip(),
                        "total_invoice": row.noOfInvoices,
                    }
                    repayment_obj = {
                        "requestId": str(row.requestID).strip(),
                        "ledgerNo": str(row.ledgerNo).strip(),
                        "borrowerCategory": str(row.borrowerCategory).strip(),
                    }

                else:
                    if str(disburse_obj.get('requestId')).strip() != str(row.requestID).strip():
                        disburse_request_packet.append(disburse_obj)
                        repay_request_packet.append(repayment_obj)

                        disburse_obj = {
                            "requestId": str(row.requestID).strip(),
                            "ledgerNo": str(row.ledgerNo).strip(),
                            "lenderCategory": str(row.lenderCategory).strip(),
                            "lenderName": str(row.lenderName).strip(),
                            "lenderCode": str(row.lenderCode).strip(),
                            "total_invoice": row.noOfInvoices,
                        }

                        repayment_obj = {
                            "requestId": str(row.requestID).strip(),
                            "ledgerNo": str(row.ledgerNo).strip(),
                            "borrowerCategory": str(row.borrowerCategory).strip(),
                        }

                        disburse_ledger_data_list = []
                        repayment_ledger_data_list = []

                        total_row_invoices = 0
                        total_invoice_request_count = 0

                if index == df.shape[0] - 1 and str(disburse_obj.get('requestId')).strip() == str(
                        row.requestID).strip():
                    disburse_request_packet.append(disburse_obj)
                    repay_request_packet.append(repayment_obj)

                if str(row.invoiceNo).strip() or str(row.invoiceDate).strip() or str(row.invoiceAmt).strip() or str(
                        row.disbursedAmt).strip():
                    disburse_ledger_dict = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "disbursedFlag": str(row.disbursedFlag).strip(),
                        "disbursedAmt": str(row.disbursedAmt).strip(),
                        "disbursedDate": str(row.disbursedDate).strip(),
                        "dueAmt": str(row.dueAmt).strip(),
                        "dueDate": str(row.dueDate).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip()
                    }
                    repayment_ledger_dict = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "assetClassification": str(row.assetClassification).strip(),
                        "dueAmt": str(row.dueAmt).strip(),
                        "dueDate": str(row.dueDate).strip(),
                        "repaymentType": str(row.repaymentType).strip(),
                        "repaymentFlag": str(row.repaymentFlag).strip(),
                        "repaymentAmt": str(row.repaymentAmt).strip(),
                        "repaymentDate": str(row.repaymentDate).strip(),
                        "pendingDueAmt": str(row.pendingDueAmt).strip(),
                        "dpd": str(row.dpd).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip()
                    }
                    total_row_invoices = total_row_invoices + 1

                if disburse_ledger_dict:
                    disburse_ledger_data_list.append(disburse_ledger_dict)
                    repayment_ledger_data_list.append(repayment_ledger_dict)
                    disburse_obj.update({
                        "ledgerData": disburse_ledger_data_list,
                        "channel": str(row.channel).strip(),
                        "hubId": str(row.hubId).strip(),
                        "idpId": str(row.idpId).strip(),
                    })

                disburse_obj.update({"ledgerData": disburse_ledger_data_list})
                repayment_obj.update({"ledgerData": repayment_ledger_data_list})

            logger.info(f"disburse request packet {disburse_request_packet}")
            logger.info(f"Repayment request packet {repay_request_packet}")
            request_packat = [{
                'disburse_request_packet': disburse_request_packet,
                'repayment_request_packet': repay_request_packet
            }]
            return request_packat
        except Exception as e:
            logger.exception(f"Exception disbursement_repayment_read_csv_file :: {e}")
            return [{"code": 500, "message": str(e)}]

    @staticmethod
    def do_disbursement_and_repayment(request_packets):
        db = next(get_db())
        from routers.finance import sync_repayment, sync_disbursement
        try:
            if request_packets and request_packets[0] and request_packets[0].get("code", '') == 500:
                return request_packets

            disburse_request_packets = request_packets[0].get('disburse_request_packet', [])
            repayment_request_packets = request_packets[0].get('repayment_request_packet', [])

            response_csv_data = []
            if request_packets[0].get("code", ''):
                response_data = {
                    "channel": "",
                    "hubId": "",
                    "idpId": "",
                    "requestId": "",
                    "APIName": "syncDisbursement",
                    "ledgerNo": "",
                    "code": request_packets[0].get("code", ''),
                    "message": request_packets[0].get("message", '')
                }
                response_csv_data.append(response_data)
                logger.info(f"don't do_disbursement_and_repayment :: if payload have error :: {response_csv_data}")
                return response_csv_data
            for disburse_request_packet in disburse_request_packets:
                response_data = {
                    "channel": disburse_request_packet.get('channel'),
                    "hubId": disburse_request_packet.get('hubId'),
                    "idpId": disburse_request_packet.get('idpId'),
                    "requestId": disburse_request_packet.get('requestId'),
                    "APIName": "syncDisbursement",
                    "ledgerNo": "",
                    "code": "",
                    "message": ""
                }

                mer_unique_id = disburse_request_packet.get('idpId', '')
                merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()

                validate_column_response = validate_column(disburse_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                merchant_key = merchant_details.merchant_key

                disburse_request_packet.pop("total_invoice")
                dis_request_packet_copy = copy.deepcopy(disburse_request_packet)
                dis_request_packet_copy.pop("channel")
                dis_request_packet_copy.pop("hubId")
                dis_request_packet_copy.pop("idpId")

                dis_signature = utils.create_signature(dis_request_packet_copy, merchant_details.merchant_secret)
                disburse_request_packet.update({"signature": dis_signature})
                logger.info(f"getting disbursement request packet :: {disburse_request_packet}")
                try:
                    dis_pay_load = AsyncDisburseSchema(**disburse_request_packet)
                except HTTPException as httpExc:
                    logger.info(f"httpExc:: {httpExc.detail}")
                    dis_pay_load = {"code": 400, "error": str(httpExc.detail)}
                except Exception as e:
                    logger.info(f" Exception:: {e}")
                    dis_pay_load = {"code": 500, "error": str(e)}

                if isinstance(dis_pay_load, dict) and dis_pay_load.get('error'):
                    response_data.update({"code": dis_pay_load.get('code'), "message": dis_pay_load.get('error')})
                    response_csv_data.append(response_data)
                    continue

                dis_api_response = sync_disbursement(merchant_key, dis_pay_load, db)
                logger.info(f"Disbursement response:: {dis_api_response}")
                response_data.update({
                    "APIName": "syncDisbursement",
                    "code": dis_api_response.get('code'),
                    "message": dis_api_response.get('message'),
                    "ledgerNo": dis_api_response.get('ledgerNo', '')
                })
                response_csv_data.append(response_data)
                api_request_obj = db.query(
                    models.APIRequestLog
                ).filter(
                    models.APIRequestLog.request_id == disburse_request_packet.get('requestId')
                ).first()
                if api_request_obj:
                    data = copy.deepcopy(api_request_obj.extra_data)
                    data.update({
                        "channel": disburse_request_packet.get('channel'),
                        "hubId": disburse_request_packet.get('hubId'),
                        "idpId": disburse_request_packet.get('idpId')
                        # "groupingId": finance_request_packet.get('groupingId')
                    })
                    api_request_obj.extra_data = data
                    db.commit()
                    db.refresh(api_request_obj)

                # if dis_api_response.get('code') == 1027:
                # Repayment
                for repayment_request_packet in repayment_request_packets:
                    if repayment_request_packet.get('requestId') == disburse_request_packet.get('requestId'):
                        repayment_response_data = {
                            "channel": disburse_request_packet.get('channel'),
                            "hubId": disburse_request_packet.get('hubId'),
                            "idpId": disburse_request_packet.get('idpId'),
                            "requestId": disburse_request_packet.get('requestId'),
                            "APIName": "syncRepayment",
                            "ledgerNo": disburse_request_packet.get('ledgerNo'),
                            "code": "",
                            "message": ""
                        }

                        repay_req_id = 'repay_' + repayment_request_packet.get('requestId')
                        items = list(repayment_request_packet.items())
                        items.insert(1, ("requestId", repay_req_id))
                        # items.insert(2, ("ledgerNo", reg_api_response.get('ledgerNo')))
                        repayment_request_packet = dict(items)

                        repay_signature = utils.create_signature(repayment_request_packet,
                                                                 merchant_details.merchant_secret)
                        repayment_request_packet.update({"signature": repay_signature})
                        logger.info(f"Repayment request packet {repayment_request_packet}")
                        try:
                            repay_pay_load = AsyncRepaymentSchema(**repayment_request_packet)
                        except HTTPException as httpExc:
                            logger.info(f"httpExc:: {httpExc.detail}")
                            repay_pay_load = {"code": 400, "error": str(httpExc.detail)}
                        except Exception as e:
                            logger.info(f" Exception:: {e}")
                            repay_pay_load = {"code": 500, "error": str(e)}

                        if isinstance(repay_pay_load, dict) and repay_pay_load.get('error', ''):
                            repayment_response_data.update({"code": repay_pay_load.get('code'),
                                                            "message": repay_pay_load.get('error')})
                            response_csv_data.append(repayment_response_data)
                            continue

                        # call  api if ledger is disbursed successfully
                        repay_api_resp = sync_repayment(merchant_key, repay_pay_load, db)
                        logger.info(f"repay_api_resp : {repay_api_resp}")
                        repayment_response_data.update({
                            "code": repay_api_resp.get('code'),
                            "message": repay_api_resp.get('message')})
                        response_csv_data.append(repayment_response_data)

                        api_request_obj = db.query(
                            models.APIRequestLog
                        ).filter(
                            models.APIRequestLog.request_id == repay_req_id
                        ).first()
                        if api_request_obj:
                            data = copy.deepcopy(api_request_obj.extra_data)
                            data.update({
                                "channel": disburse_request_packet.get('channel'),
                                "hubId": disburse_request_packet.get('hubId'),
                                "idpId": disburse_request_packet.get('idpId')
                                # "groupingId": finance_request_packet.get('groupingId')
                            })
                            api_request_obj.extra_data = data
                            db.commit()
                            db.refresh(api_request_obj)
            return response_csv_data
        except Exception as e:
            logger.exception(f"Exception do_disbursement_and_repayment :: {e}")
            return {
                **ErrorCodes.get_error_response(500)
            }


class RegistrationWithECFinanceDisbursementVaiCSV:
    @staticmethod
    def read_csv_to_reg_with_ec_fin_dis(df):
        total_row_invoices = 0
        reg_request_packet = []

        finance_request_packet = []
        disburse_request_packet = []

        registration_obj = {}
        reg_ledger_data_list = []

        finance_obj = {}
        finance_ledger_data_list = []

        disburse_obj = {}
        disburse_ledger_data_list = []

        error_response = {"code": "", "message": ""}
        expected_headers = ["channel", "hubId", "idpId", "requestID", "groupingId", "sellerCode", "buyerCode",
                            "sellerGst", "buyerGst", "ledgerAmtFlag", "lenderCategory", "lenderName", "lenderCode",
                            "borrowerCategory", "noOfInvoices", "validationType", "validationRefNo", "invoiceNo",
                            "invoiceDate", "invoiceAmt", "verifyGSTNFlag", "invoiceDueDate", "financeRequestAmt",
                            "financeRequestDate", "dueDate", "fundingAmtFlag", "adjustmentType", "adjustmentAmt",
                            "disbursedFlag", "disbursedAmt", "disbursedDate", "dueAmt"]

        actual_headers = df.columns.tolist()
        extra_headers = set(expected_headers) - set(actual_headers)
        # extra_headers = set(actual_headers) - set(expected_headers)
        if extra_headers:
            error_response.update(**ErrorCodes.get_error_response(1090))
            return [error_response]

        try:
            hub_validation = True
            for index, row in df.iterrows():
                reg_ledger_datas = ""
                finance_ledger_datas = ""
                disburse_ledger_datas = ""
                logger.info(f" row.noOfInvoices {row.noOfInvoices}")
                row.noOfInvoices = utils.cast_to_int_or_zero(row.noOfInvoices)
                # initial registration_obj = {}, it means very first time reg obj prepare
                if not registration_obj:
                    registration_obj = {
                        "requestId": str(row.requestID).strip(),
                        "sellerCode": str(row.sellerCode).strip(),
                        "buyerCode": str(row.buyerCode).strip(),
                        "sellerGst": str(row.sellerGst).strip(),
                        "buyerGst": str(row.buyerGst).strip(),
                        "groupingId": str(row.groupingId).strip(),
                        "total_invoice": row.noOfInvoices,
                        "channel": row.channel.strip(),
                        "hubId": row.hubId.strip(),
                        "idpId": row.idpId.strip()
                    }
                    finance_obj = {
                        "requestId": str(row.requestID).strip(),
                        "ledgerAmtFlag": str(row.ledgerAmtFlag).strip(),
                        "lenderCategory": str(row.lenderCategory).strip(),
                        "lenderName": str(row.lenderName).strip(),
                        "lenderCode": str(row.lenderCode).strip(),
                        "borrowerCategory": str(row.borrowerCategory).strip(),
                    }
                    disburse_obj = {
                        "requestId": str(row.requestID).strip(),
                        "lenderCategory": str(row.lenderCategory).strip(),
                        "lenderName": str(row.lenderName).strip(),
                        "lenderCode": str(row.lenderCode).strip(),
                    }
                else:

                    if str(registration_obj.get('requestId')).strip() != str(row.requestID).strip():
                        if hub_validation:
                            reg_request_packet.append(registration_obj)
                            finance_request_packet.append(finance_obj)
                            disburse_request_packet.append(disburse_obj)
                        hub_validation = True

                        registration_obj = {
                            "requestId": str(row.requestID).strip(),
                            "sellerCode": str(row.sellerCode).strip(),
                            "buyerCode": str(row.buyerCode).strip(),
                            "sellerGst": str(row.sellerGst).strip(),
                            "buyerGst": str(row.buyerGst).strip(),
                            "groupingId": str(row.groupingId).strip(),
                            "total_invoice": row.noOfInvoices,
                            "channel": row.channel.strip(),
                            "hubId": row.hubId.strip(),
                            "idpId": row.idpId.strip()
                        }

                        finance_obj = {
                            "requestId": str(row.requestID).strip(),
                            "ledgerAmtFlag": str(row.ledgerAmtFlag).strip(),
                            "lenderCategory": str(row.lenderCategory).strip(),
                            "lenderName": str(row.lenderName).strip(),
                            "lenderCode": str(row.lenderCode).strip(),
                            "borrowerCategory": str(row.borrowerCategory).strip(),
                        }
                        disburse_obj = {
                            "requestId": str(row.requestID).strip(),
                            "lenderCategory": str(row.lenderCategory).strip(),
                            "lenderName": str(row.lenderName).strip(),
                            "lenderCode": str(row.lenderCode).strip(),
                        }

                        reg_ledger_data_list = []
                        finance_ledger_data_list = []
                        disburse_ledger_data_list = []
                        total_row_invoices = 0

                if index == df.shape[0] - 1 and str(registration_obj.get('requestId')) == str(row.requestID):
                    if hub_validation:
                        reg_request_packet.append(registration_obj)
                        finance_request_packet.append(finance_obj)
                        disburse_request_packet.append(disburse_obj)
                    hub_validation = True

                gst_status = str(row.verifyGSTNFlag).strip()
                if row.verifyGSTNFlag == str(1):
                    gst_status = True
                if row.verifyGSTNFlag == str(0):
                    gst_status = False

                if (str(row.invoiceNo).strip() or str(row.invoiceDate).strip() or str(row.invoiceAmt).strip()
                    or str(row.dueDate).strip()) or gst_status:
                    reg_ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip(),
                        "verifyGSTNFlag": gst_status,
                        "invoiceDueDate": row.invoiceDueDate,
                    }
                    finance_ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "financeRequestAmt": str(row.financeRequestAmt).strip(),
                        "financeRequestDate": str(row.financeRequestDate).strip(),
                        "dueDate": str(row.dueDate).strip(),
                        "fundingAmtFlag": str(row.fundingAmtFlag).strip(),
                        "adjustmentType": str(row.adjustmentType).strip(),
                        "adjustmentAmt": str(row.adjustmentAmt).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip()
                    }
                    disburse_ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "disbursedFlag": str(row.disbursedFlag).strip(),
                        "disbursedAmt": str(row.disbursedAmt).strip(),
                        "disbursedDate": str(row.disbursedDate).strip(),
                        "dueAmt": str(row.dueAmt).strip(),
                        "dueDate": str(row.dueDate).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip()
                    }
                    total_row_invoices = total_row_invoices + 1

                if reg_ledger_datas:
                    reg_ledger_data_list.append(reg_ledger_datas)
                    finance_ledger_data_list.append(finance_ledger_datas)
                    disburse_ledger_data_list.append(disburse_ledger_datas)

                if reg_ledger_datas:
                    registration_obj.update({"ledgerData": reg_ledger_data_list})
                    finance_obj.update({"ledgerData": finance_ledger_data_list})
                    disburse_obj.update({"ledgerData": disburse_ledger_data_list})

                    if (registration_obj.get('channel').lower() != row.channel.lower().strip()
                            or registration_obj.get('hubId').strip() != row.hubId.strip()
                            or registration_obj.get('idpId').strip() != row.idpId.strip()
                            or registration_obj.get('sellerCode').strip() != row.sellerCode.strip()
                            or registration_obj.get('buyerCode').strip() != row.buyerCode.strip()
                    ):
                        hub_validation = False

                    if registration_obj.get('sellerGst').strip() or registration_obj.get('buyerGst').strip():
                        if (registration_obj.get('sellerGst').strip() != row.sellerGst.strip()
                                or registration_obj.get('buyerGst').strip() != row.buyerGst.strip()):
                            hub_validation = False

            logger.info(f"read_csv_to_reg_with_ec_fin_dis :: Request Data from csv")
            logger.info(f"Reg request packet {reg_request_packet}")
            logger.info(f"Finance request packet {finance_request_packet}")
            logger.info(f"Disburse request packet {disburse_request_packet}")
            if len(reg_request_packet) == 0:
                request_packet = [{"code": 500, "message": "incorrect data"}]
            else:
                request_packet = [{
                    "registration_request_packet": reg_request_packet or [{}],
                    "finance_request_packet": finance_request_packet or [{}],
                    "disburse_request_packet": disburse_request_packet or [{}]
                }]
            return request_packet
        except Exception as e:
            logger.exception(f"Exception read_csv_to_reg_with_ec_fin_dis :: {e}")
            return [{"code": 500, "message": str(e)}]

    @staticmethod
    def do_reg_with_ec_fin_disb(request_packets):
        db = next(get_db())
        from routers.registration import sync_invoice_registration_with_code
        from routers.finance import sync_finance, sync_disbursement
        try:
            response_csv_data = []
            request_packets = request_packets[0]
            if request_packets.get('code', ''):
                response_data = {
                    "channel": '',
                    "hubId": '',
                    "idpId": '',
                    "requestID": '',
                    "groupingId": '',
                    "APIName": "Invoice Reg With EC",
                    "ledgerNo": "",
                    "code": request_packets.get('code', ''),
                    "message": request_packets.get('message', ''),
                    "invoiceNo": "",
                    "invoiceDate": "",
                    "invoiceAmt": "",
                    "invoiceStatus": "",
                    "fundedAmt": "",
                    "gstVerificationStatus": ""
                }
                response_csv_data.append(response_data)
                logger.info(f"don't, do_reg_with_ec_fin_disb :: if payload have error :: {request_packets}")
                return response_csv_data

            registration_request_packets = request_packets.get("registration_request_packet")
            finance_request_packets = request_packets.get("finance_request_packet")
            disburse_request_packets = request_packets.get("disburse_request_packet")

            for reg_request_packet in registration_request_packets:
                logger.info(f"let's process:: Registration request packet :: ........")
                response_data = {
                    "channel": reg_request_packet.get('channel'),
                    "hubId": reg_request_packet.get('hubId'),
                    "idpId": reg_request_packet.get('idpId'),
                    "requestId": reg_request_packet.get('requestId'),
                    "groupingId": reg_request_packet.get('groupingId'),
                    "APIName": "Invoice Reg With EC",
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
                mer_unique_id = reg_request_packet.get('idpId', '')
                merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()

                validate_column_response = validate_column(reg_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                merchant_key = merchant_details.merchant_key
                reg_request_packet.pop('total_invoice')

                reg_request_packet_copy = copy.deepcopy(reg_request_packet)
                reg_request_packet_copy.pop("channel")
                reg_request_packet_copy.pop("hubId")
                reg_request_packet_copy.pop("idpId")

                reg_signature = utils.create_signature(reg_request_packet_copy, merchant_details.merchant_secret)
                reg_request_packet.update({"signature": reg_signature})
                logger.info(f"getting registration request packet {reg_request_packet}")
                try:
                    reg_pay_load = AsyncInvoiceRegistrationWithCodeSchema(**reg_request_packet)
                except HTTPException as httpExc:
                    logger.info(f"httpExc:: {httpExc.detail}")
                    reg_pay_load = {"code": 400, "error": str(httpExc.detail)}
                except Exception as e:
                    logger.info(f" Exception:: {e}")
                    reg_pay_load = {"code": 500, "error": str(e)}

                if isinstance(reg_pay_load, dict) and reg_pay_load.get('error'):
                    response_data.update({"code": reg_pay_load.get('code'), "message": reg_pay_load.get('error')})
                    response_csv_data.append(response_data)
                    continue

                reg_api_response = sync_invoice_registration_with_code(merchant_key, reg_pay_load, db)
                logger.info(f"After Registration, api response:: {reg_api_response}")
                response_data.update({
                    "APIName": "Invoice Reg With EC",
                    "code": reg_api_response.get('code'),
                    "message": reg_api_response.get('message'),
                    "ledgerNo": reg_api_response.get('ledgerNo', '')
                })
                response_csv_data.append(response_data)

                api_request_obj = db.query(
                    models.APIRequestLog
                ).filter(
                    models.APIRequestLog.request_id == reg_request_packet.get('requestId')
                ).first()
                if api_request_obj:
                    data = copy.deepcopy(api_request_obj.extra_data)
                    data.update({
                        "channel": reg_request_packet.get('channel'),
                        "hubId": reg_request_packet.get('hubId'),
                        "idpId": reg_request_packet.get('idpId'),
                        "groupingId": reg_request_packet.get('groupingId')
                    })
                    api_request_obj.extra_data = data
                    db.commit()
                    db.refresh(api_request_obj)

                if reg_api_response.get('code') == 200:
                    # Financing
                    for finance_request_packet in finance_request_packets:
                        if finance_request_packet.get('requestId') == reg_request_packet.get('requestId'):
                            finance_response_data = {
                                "channel": reg_request_packet.get('channel'),
                                "hubId": reg_request_packet.get('hubId'),
                                "idpId": reg_request_packet.get('idpId'),
                                "requestId": reg_request_packet.get('requestId'),
                                "groupingId": reg_request_packet.get('groupingId'),
                                "APIName": "syncFinancing",
                                "ledgerNo": reg_api_response.get('ledgerNo'),
                                "code": "",
                                "message": "",
                                "invoiceNo": finance_request_packet.get('invoiceNo'),
                                "invoiceDate": finance_request_packet.get('invoiceDate'),
                                "invoiceAmt": finance_request_packet.get('invoiceAmt'),
                                "invoiceStatus": "",
                                "fundedAmt": "",
                                "gstVerificationStatus": ""
                            }

                            fin_req_id = 'finance_' + finance_request_packet.get('requestId')
                            items = list(finance_request_packet.items())
                            items.insert(1, ("requestId", fin_req_id))
                            items.insert(2, ("ledgerNo", reg_api_response.get('ledgerNo')))
                            finance_request_packet = dict(items)

                            finance_signature = utils.create_signature(finance_request_packet,
                                                                       merchant_details.merchant_secret)
                            finance_request_packet.update({"signature": finance_signature})
                            logger.info(f"finance request packet {finance_request_packet}")
                            try:
                                fin_pay_load = AsyncBulkFinanceSchema(**finance_request_packet)
                            except HTTPException as httpExc:
                                logger.info(f"httpExc:: {httpExc.detail}")
                                fin_pay_load = {"code": 400, "error": str(httpExc.detail)}
                            except Exception as e:
                                logger.info(f" Exception:: {e}")
                                fin_pay_load = {"code": 500, "error": str(e)}

                            if isinstance(fin_pay_load, dict) and fin_pay_load.get('error', ''):
                                finance_response_data.update({
                                    "code": fin_pay_load.get('code'),
                                    "message": fin_pay_load.get('error')
                                })
                                response_csv_data.append(finance_response_data)
                                continue

                            finance_api_resp = sync_finance(merchant_key, fin_pay_load, db)
                            logger.info(f"After, Reg & finance api response :: {finance_api_resp}")

                            api_request_obj = db.query(
                                models.APIRequestLog
                            ).filter(
                                models.APIRequestLog.request_id == fin_req_id
                            ).first()
                            if api_request_obj:
                                data = copy.deepcopy(api_request_obj.extra_data)
                                data.update({
                                    "channel": reg_request_packet.get('channel'),
                                    "hubId": reg_request_packet.get('hubId'),
                                    "idpId": reg_request_packet.get('idpId'),
                                    "groupingId": reg_request_packet.get('groupingId')
                                })
                                api_request_obj.extra_data = data
                                db.commit()
                                db.refresh(api_request_obj)

                            # write finance api resp to csv
                            if finance_api_resp.get('ledgerData'):
                                for fin_ledger_data in finance_api_resp.get('ledgerData'):
                                    finance_response_data = {
                                        "channel": reg_request_packet.get('channel'),
                                        "hubId": reg_request_packet.get('hubId'),
                                        "idpId": reg_request_packet.get('idpId'),
                                        "requestId": reg_request_packet.get('requestId'),
                                        "groupingId": reg_request_packet.get('groupingId'),
                                        "APIName": "syncFinancing",
                                        "ledgerNo": reg_api_response.get('ledgerNo'),
                                        "code": finance_api_resp.get('code'),
                                        "message": finance_api_resp.get('message'),
                                        "invoiceNo": fin_ledger_data.get('invoiceNo'),
                                        "invoiceDate": fin_ledger_data.get('invoiceDate'),
                                        "invoiceAmt": fin_ledger_data.get('invoiceAmt'),
                                        "invoiceStatus": fin_ledger_data.get('invoiceStatus'),
                                        "fundedAmt": fin_ledger_data.get('fundedAmt'),
                                        "gstVerificationStatus": fin_ledger_data.get('gstVerificationStatus')
                                    }
                                    response_csv_data.append(finance_response_data)
                            else:
                                finance_response_data = {
                                    "channel": reg_request_packet.get('channel'),
                                    "hubId": reg_request_packet.get('hubId'),
                                    "idpId": reg_request_packet.get('idpId'),
                                    "requestId": reg_request_packet.get('requestId'),
                                    "groupingId": reg_request_packet.get('groupingId'),
                                    "APIName": "syncFinancing",
                                    "ledgerNo": reg_api_response.get('ledgerNo'),
                                    "code": finance_api_resp.get('code'),
                                    "message": finance_api_resp.get('message'),
                                    "invoiceNo": "",
                                    "invoiceDate": "",
                                    "invoiceAmt": "",
                                    "invoiceStatus": "",
                                    "fundedAmt": "",
                                    "gstVerificationStatus": ""
                                }
                                response_csv_data.append(finance_response_data)

                            # call disburse api if ledger is funded successfully
                            if finance_api_resp.get('code') in [1013]:
                                for disburse_request_packet in disburse_request_packets:
                                    if disburse_request_packet.get('requestId') == reg_request_packet.get('requestId'):
                                        dis_response_data = {
                                            "channel": reg_request_packet.get('channel'),
                                            "hubId": reg_request_packet.get('hubId'),
                                            "idpId": reg_request_packet.get('idpId'),
                                            "requestId": reg_request_packet.get('requestId'),
                                            "groupingId": reg_request_packet.get('groupingId'),
                                            "APIName": "syncDisbursement",
                                            "ledgerNo": reg_api_response.get('ledgerNo'),
                                            "code": "",
                                            "message": "",
                                            "invoiceNo": "",
                                            "invoiceDate": "",
                                            "invoiceAmt": "",
                                            "invoiceStatus": "",
                                            "fundedAmt": "",
                                            "gstVerificationStatus": ""
                                        }

                                        dis_req_id = 'disburse_' + finance_request_packet.get('requestId')
                                        items = list(disburse_request_packet.items())
                                        items.insert(1, ("requestId", dis_req_id))
                                        items.insert(2, ("ledgerNo", reg_api_response.get('ledgerNo')))
                                        disburse_request_packet = dict(items)

                                        disburse_signature = utils.create_signature(disburse_request_packet,
                                                                                    merchant_details.merchant_secret)
                                        disburse_request_packet.update({"signature": disburse_signature})
                                        logger.info(f"disburse request packet {disburse_request_packet}")

                                        try:
                                            dis_pay_load = AsyncDisburseSchema(**disburse_request_packet)
                                        except HTTPException as httpExc:
                                            logger.info(f"httpExc:: {httpExc.detail}")
                                            dis_pay_load = {"code": 400, "error": str(httpExc.detail)}
                                        except Exception as e:
                                            logger.info(f" Exception:: {e}")
                                            dis_pay_load = {"code": 500, "error": str(e)}

                                        if isinstance(dis_pay_load, dict) and dis_pay_load.get('error', ''):
                                            dis_response_data.update({
                                                "code": dis_pay_load.get('code'),
                                                "message": dis_pay_load.get('error')
                                            })
                                            response_csv_data.append(dis_response_data)
                                            continue

                                        disburse_api_resp = sync_disbursement(merchant_key, dis_pay_load, db)
                                        dis_response_data.update({
                                            "code": disburse_api_resp.get('code'),
                                            "message": disburse_api_resp.get('message')
                                        })
                                        logger.info(f"After, Reg & fin, Dis api response :: {disburse_api_resp}")
                                        response_csv_data.append(dis_response_data)
                                        api_request_obj = db.query(
                                            models.APIRequestLog
                                        ).filter(
                                            models.APIRequestLog.request_id == dis_req_id
                                        ).first()
                                        if api_request_obj:
                                            data = copy.deepcopy(api_request_obj.extra_data)
                                            data.update({
                                                "channel": reg_request_packet.get('channel'),
                                                "hubId": reg_request_packet.get('hubId'),
                                                "idpId": reg_request_packet.get('idpId'),
                                                "groupingId": reg_request_packet.get('groupingId')
                                            })
                                            api_request_obj.extra_data = data
                                            db.commit()
                                            db.refresh(api_request_obj)

            return response_csv_data
        except Exception as e:
            logger.exception(f"Exception do_reg_with_ec_fin_disb :: {e}")
            return {
                **ErrorCodes.get_error_response(500)
            }


class ValidateWithEntityCodeVaiCSV:

    @staticmethod
    def read_csv_to_validation_service_with_code(df):
        total_row_invoices = 0
        val_ser_with_ec_request_packet = []
        val_ser_with_ec_obj = {}
        val_ser_ledger_data_list = []
        val_ser_ledger_datas = []

        error_response = {"code": "", "message": ""}
        expected_headers = SFTP_API_HEADERS.validate_with_entity_code

        actual_headers = df.columns.tolist()
        extra_headers = set(expected_headers) - set(actual_headers)
        if extra_headers:
            error_response.update(**ErrorCodes.get_error_response(1090))
            return [error_response]

        try:
            logger.info(f" read_csv_to_validation_service_with_code :: ............")
            for index, row in df.iterrows():
                logger.info(f" row.noOfInvoices {row.noOfInvoices}")

                if row.noOfInvoices:
                    row.noOfInvoices = utils.cast_to_int_or_zero(row.noOfInvoices)

                # initial val_ser_with_ec_obj = {}, it means very first time val ser obj prepare
                if not val_ser_with_ec_obj:
                    val_ser_with_ec_obj = {
                        "requestId": str(row.requestID).strip(),
                        "sellerCode": str(row.sellerCode).strip(),
                        "buyerCode": str(row.buyerCode).strip(),
                        "sellerGst": str(row.sellerGst).strip(),
                        "buyerGst": str(row.buyerGst).strip(),
                        "groupingId": str(row.groupingId).strip(),
                        "total_invoice": row.noOfInvoices,
                    }
                else:
                    if str(val_ser_with_ec_obj.get('requestId')).strip() != str(row.requestID).strip():
                        val_ser_with_ec_request_packet.append(val_ser_with_ec_obj)

                        val_ser_with_ec_obj = {
                            "requestId": str(row.requestID).strip(),
                            "sellerCode": str(row.sellerCode).strip(),
                            "buyerCode": str(row.buyerCode).strip(),
                            "sellerGst": str(row.sellerGst).strip(),
                            "buyerGst": str(row.buyerGst).strip(),
                            "groupingId": str(row.groupingId).strip(),
                            "total_invoice": row.noOfInvoices,
                        }
                        val_ser_ledger_data_list = []
                        total_row_invoices = 0

                if index == df.shape[0] - 1 and str(val_ser_with_ec_obj.get('requestId')).strip() == str(
                        row.requestID).strip():
                    val_ser_with_ec_request_packet.append(val_ser_with_ec_obj)

                if (str(row.invoiceNo).strip() or str(row.invoiceDate).strip()
                        or str(row.invoiceAmt).strip() or str(row.dueDate).strip()):
                    gst_status = row.verifyGSTNFlag
                    if row.verifyGSTNFlag == str(1):
                        gst_status = True
                    if row.verifyGSTNFlag == str(0):
                        gst_status = False
                    val_ser_ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo).strip(),
                        "invoiceDate": str(row.invoiceDate).strip(),
                        "invoiceAmt": str(row.invoiceAmt).strip(),
                        "verifyGSTNFlag": gst_status,
                        "invoiceDueDate": str(row.invoiceDueDate).strip()
                    }
                    total_row_invoices = total_row_invoices + 1

                if val_ser_ledger_datas:
                    val_ser_ledger_data_list.append(val_ser_ledger_datas)

                if val_ser_ledger_datas:
                    val_ser_with_ec_obj.update({
                        "ledgerData": val_ser_ledger_data_list,
                        "channel": str(row.channel).strip(),
                        "hubId": str(row.hubId).strip(),
                        "idpId": str(row.idpId).strip()
                    })
                val_ser_with_ec_obj.update({"ledgerData": val_ser_ledger_data_list})
            logger.info(f"read_csv_to_validation_service_with_code : response :: {val_ser_with_ec_request_packet}")
            return val_ser_with_ec_request_packet
        except Exception as e:
            logger.exception(f"Exception read_csv_to_validation_service_with_code :: {e}")
            # raise HTTPException(status_code=404, detail="Exception found")
            return [{"code": 500, "message": str(e)}]

    @staticmethod
    def do_validation_service_with_code(val_ser_request_packets):
        db = next(get_db())
        try:
            logger.info(f"do_validation_service_with_code :: ...........")
            response_csv_data = []
            if val_ser_request_packets[0].get("code", ''):
                response_data = {
                    "channel": "",
                    "hubId": "",
                    "idpId": "",
                    "requestId": "",
                    "code": val_ser_request_packets[0].get("code", ''),
                    "message": val_ser_request_packets[0].get("message", ''),
                    "groupingId": ""
                }
                response_csv_data.append(response_data)
                logger.info(f"don't do_validation_service_with_code :: if payload have error :: {response_csv_data}")
                return response_csv_data

            for val_ser_request_packet in val_ser_request_packets:
                logger.info(f"let's process:: validate service request packet :: ........")
                response_data = {
                    "channel": val_ser_request_packet.get('channel'),
                    "hubId": val_ser_request_packet.get('hubId'),
                    "idpId": val_ser_request_packet.get('idpId'),
                    "requestId": val_ser_request_packet.get('requestId'),
                    "code": "",
                    "message": "",
                    "groupingId": val_ser_request_packet.get('groupingId', ''),
                }

                mer_unique_id = val_ser_request_packet.get('idpId', '')
                merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()

                validate_column_response = validate_column(val_ser_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                merchant_key = merchant_details.merchant_key

                val_ser_request_packet.pop("total_invoice")
                val_ser_request_packet_copy = copy.deepcopy(val_ser_request_packet)
                val_ser_request_packet_copy.pop("channel")
                val_ser_request_packet_copy.pop("hubId")
                val_ser_request_packet_copy.pop("idpId")

                validation_ser_signature = utils.create_signature(val_ser_request_packet_copy,
                                                                  merchant_details.merchant_secret)
                val_ser_request_packet.update({"signature": validation_ser_signature})

                logger.info(f"getting validation service with ec request packet :: {val_ser_request_packet}")
                try:
                    val_ser_pay_load = AsyncInvoiceRegistrationWithCodeSchema(**val_ser_request_packet)
                except HTTPException as httpExc:
                    logger.info(f"httpExc:: {httpExc.detail}")
                    val_ser_pay_load = {"code": 400, "error": str(httpExc.detail)}
                except Exception as e:
                    logger.info(f" Exception:: {e}")
                    val_ser_pay_load = {"code": 500, "error": str(e)}

                if isinstance(val_ser_pay_load, dict) and val_ser_pay_load.get('error'):
                    response_data.update(
                        {"code": val_ser_pay_load.get('code'), "message": val_ser_pay_load.get('error')})
                    response_csv_data.append(response_data)
                    continue

                val_ser_api_resp = sync_validation_service_with_code(merchant_key, val_ser_pay_load, db)
                logger.info(f"After sync_validation_service_with_code, api response:: {val_ser_api_resp}")
                api_request_obj = db.query(
                    models.APIRequestLog
                ).filter(
                    models.APIRequestLog.request_id == val_ser_request_packet.get('requestId')
                ).first()
                if api_request_obj:
                    data = copy.deepcopy(api_request_obj.extra_data)
                    data.update({
                        "channel": val_ser_request_packet.get('channel'),
                        "hubId": val_ser_request_packet.get('hubId'),
                        "idpId": val_ser_request_packet.get('idpId'),
                        "code": val_ser_api_resp.get('code', ''),
                        "message": val_ser_api_resp.get('message', ''),
                        # "groupingId": val_ser_request_packet.get('groupingId')
                    })
                    api_request_obj.extra_data = data
                    db.commit()
                    db.refresh(api_request_obj)

                response_data = {
                    "channel": val_ser_request_packet.get('channel'),
                    "hubId": val_ser_request_packet.get('hubId'),
                    "idpId": val_ser_request_packet.get('idpId'),
                    "requestId": val_ser_request_packet.get('requestId'),
                    "code": val_ser_api_resp.get('code', ''),
                    "message": val_ser_api_resp.get('message', ''),
                    "groupingId": val_ser_request_packet.get('groupingId', '')

                }
                response_csv_data.append(response_data)
            return response_csv_data
        except Exception as e:
            logger.exception(f"Exception do_financing :: {e}")
            return {
                **ErrorCodes.get_error_response(500)
            }


class ValidateWithoutEntityCodeVaiCSV:

    @staticmethod
    def read_csv_to_validation_service_without_code(df):
        invoice_obj = {}
        ledger_data = []
        seller_identifier_data = []
        buyer_identifier_data = []
        request_packet = []
        total_row_invoices = 0
        seller_identifier_rows = 0
        buyer_identifier_rows = 0
        error_response = {"code": "", "message": ""}
        expected_headers = SFTP_API_HEADERS.validate_without_entity_code

        actual_headers = df.columns.tolist()
        extra_headers = set(expected_headers) - set(actual_headers)
        if extra_headers:
            error_response.update(**ErrorCodes.get_error_response(1090))
            return [error_response]
        try:
            request_packet_validation = True
            for index, row in df.iterrows():
                ledger_datas = ""

                if not invoice_obj:
                    invoice_obj = {
                        "requestId": str(row.requestID),
                        "sellerGst": str(row.sellerGst),
                        "buyerGst": str(row.buyerGst),
                        "channel": row.channel,
                        "hubId": row.hubId,
                        "idpId": row.idpId,
                        "groupingId": str(row.groupingId),
                        "total_invoice": row.noOfInvoices if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                        "total_buyer_identifier_request": row.buyerDataNo if row.buyerDataNo and row.buyerDataNo.isnumeric() else 0,
                        "total_buyer_identifier_count": 0,
                        "total_seller_identifier_request": row.sellerDataNo if row.sellerDataNo and row.sellerDataNo.isnumeric() else 0,
                        "total_seller_identifier_count": 0
                    }
                else:
                    if str(invoice_obj.get('requestId')) != str(row.requestID):
                        if request_packet_validation:
                            request_packet.append(invoice_obj)
                        request_packet_validation = True
                        invoice_obj = {
                            "requestId": str(row.requestID),
                            "sellerGst": str(row.sellerGst),
                            "buyerGst": str(row.buyerGst),
                            "groupingId": str(row.groupingId),
                            "channel": row.channel,
                            "hubId": row.hubId,
                            "idpId": row.idpId,
                            "total_invoice": row.noOfInvoices if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                            "total_buyer_identifier_request": row.buyerDataNo if row.buyerDataNo and row.buyerDataNo.isnumeric() else 0,
                            "total_buyer_identifier_count": 0,
                            "total_seller_identifier_request": row.sellerDataNo if row.sellerDataNo and row.sellerDataNo.isnumeric() else 0,
                            "total_seller_identifier_count": 0
                        }

                        ledger_data = []
                        seller_identifier_data = []
                        buyer_identifier_data = []
                        total_row_invoices = 0
                        seller_identifier_rows = 0
                        buyer_identifier_rows = 0

                gst_status = row.verifyGSTNFlag
                if row.verifyGSTNFlag == str(1):
                    gst_status = True
                if row.verifyGSTNFlag == str(0):
                    gst_status = False

                if str(row.invoiceNo) or row.invoiceDate or str(row.invoiceAmt) or gst_status or row.invoiceDueDate:
                    ledger_datas = {
                        "validationType": str(row.validationType).strip() if row.validationType else "",
                        "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                        "invoiceNo": str(row.invoiceNo),
                        "invoiceDate": row.invoiceDate,
                        "invoiceAmt": str(row.invoiceAmt),
                        "verifyGSTNFlag": gst_status,
                        "invoiceDueDate": row.invoiceDueDate,
                    }

                    total_row_invoices = total_row_invoices + 1

                if str(row.sellerIdType) or str(row.sellerIdNo) or str(row.sellerIdName) or str(row.sellerIfsc):
                    seller_identifier_rows = seller_identifier_rows + 1
                    invoice_obj.update({"total_seller_identifier_count": str(seller_identifier_rows)})

                seller_identifier_datas = {
                    "sellerIdType": row.sellerIdType,
                    "sellerIdNo": row.sellerIdNo,
                    "sellerIdName": row.sellerIdName,
                    "ifsc": row.sellerIfsc
                }

                if str(row.buyerIdType) or str(row.buyerIdNo) or str(row.buyerIdName) or str(row.buyerIfsc):
                    buyer_identifier_rows = buyer_identifier_rows + 1
                    invoice_obj.update({"total_buyer_identifier_count": str(buyer_identifier_rows)})

                buyer_identifier_datas = {
                    "buyerIdType": str(row.buyerIdType),
                    "buyerIdNo": str(row.buyerIdNo),
                    "buyerIdName": str(row.buyerIdName),
                    "ifsc": row.buyerIfsc
                }
                # if invoice_no !=
                if ledger_datas:
                    ledger_data.append(ledger_datas)

                if seller_identifier_datas:
                    seller_identifier_data.append(seller_identifier_datas)

                if buyer_identifier_datas:
                    buyer_identifier_data.append(buyer_identifier_datas)

                if ledger_datas:
                    invoice_obj.update({
                        "ledgerData": ledger_data,
                        "sellerIdentifierData": seller_identifier_data,
                        "buyerIdentifierData": buyer_identifier_data
                    })

                if (invoice_obj.get('channel').lower() != row.channel.lower().strip()
                        or invoice_obj.get('hubId').strip() != row.hubId.strip()
                        or invoice_obj.get('idpId') != row.idpId.strip()
                        # or invoice_obj.get('sellerGst') != row.sellerGst.strip()
                        # or invoice_obj.get('buyerGst') != row.buyerGst.strip()
                ):
                    request_packet_validation = False

                if index == df.shape[0] - 1 and str(invoice_obj.get('requestId')) == str(row.requestID):
                    if request_packet_validation:
                        if invoice_obj.get('ledgerData'):
                            request_packet.append(invoice_obj)
                    request_packet_validation = True

            logger.info(f"getting read_csv_to_validation_service_without_code request packet {request_packet}")
            return request_packet
        except Exception as e:
            logger.exception(f"Exception read_csv_to_validation_service_without_code :: {e}")
            return [{"code": 500, "message": str(e)}]

    @staticmethod
    def do_validation_service_without_code(val_ser_request_packets):
        db = next(get_db())
        try:
            logger.info(f"do_validation_service_without_code :: ...........")
            response_csv_data = []
            if val_ser_request_packets[0].get("code", ''):
                response_data = {
                    "channel": "",
                    "hubId": "",
                    "idpId": "",
                    "requestId": "",
                    "code": val_ser_request_packets[0].get("code", ''),
                    "message": val_ser_request_packets[0].get("message", ''),
                    "groupingId": ""
                }
                response_csv_data.append(response_data)
                logger.info(f"don't do_validation_service_without_code :: if payload have error :: {response_csv_data}")
                return response_csv_data

            for val_ser_request_packet in val_ser_request_packets:
                logger.info(f"let's process:: validate service request packet :: ........")
                response_data = {
                    "channel": val_ser_request_packet.get('channel'),
                    "hubId": val_ser_request_packet.get('hubId'),
                    "idpId": val_ser_request_packet.get('idpId'),
                    "requestId": val_ser_request_packet.get('requestId'),
                    "code": "",
                    "message": "",
                    "groupingId": val_ser_request_packet.get('groupingId', '')
                }

                mer_unique_id = val_ser_request_packet.get('idpId', '')
                merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()

                validate_column_response = validate_column(val_ser_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                merchant_key = merchant_details.merchant_key
                val_ser_request_packet.pop('total_buyer_identifier_request')
                val_ser_request_packet.pop('total_buyer_identifier_count')
                val_ser_request_packet.pop('total_seller_identifier_request')
                val_ser_request_packet.pop('total_seller_identifier_count')
                val_ser_request_packet.pop("total_invoice")

                val_ser_request_packet_copy = copy.deepcopy(val_ser_request_packet)
                val_ser_request_packet_copy.pop("channel")
                val_ser_request_packet_copy.pop("hubId")
                val_ser_request_packet_copy.pop("idpId")

                validation_ser_signature = utils.create_signature(val_ser_request_packet_copy,
                                                                  merchant_details.merchant_secret)
                val_ser_request_packet.update({"signature": validation_ser_signature})

                logger.info(f"getting validation service without ec request packet :: {val_ser_request_packet}")
                try:
                    val_ser_pay_load = InvoiceRequestSchema(**val_ser_request_packet)
                except HTTPException as httpExc:
                    logger.info(f"httpExc:: {httpExc.detail}")
                    val_ser_pay_load = {"code": 400, "error": str(httpExc.detail)}
                except Exception as e:
                    logger.info(f" Exception:: {e}")
                    val_ser_pay_load = {"code": 500, "error": str(e)}

                if isinstance(val_ser_pay_load, dict) and val_ser_pay_load.get('error'):
                    response_data.update(
                        {"code": val_ser_pay_load.get('code'), "message": val_ser_pay_load.get('error')})
                    response_csv_data.append(response_data)
                    continue

                val_ser_api_resp = sync_validation_service_without_code(merchant_key, val_ser_pay_load, db)
                logger.info(f"After sync_validation_service_without_code, api response:: {val_ser_api_resp}")
                api_request_obj = db.query(
                    models.APIRequestLog
                ).filter(
                    models.APIRequestLog.request_id == val_ser_request_packet.get('requestId')
                ).first()
                if api_request_obj:
                    data = copy.deepcopy(api_request_obj.extra_data)
                    data.update({
                        "channel": val_ser_request_packet.get('channel'),
                        "hubId": val_ser_request_packet.get('hubId'),
                        "idpId": val_ser_request_packet.get('idpId'),
                        "groupingId": val_ser_request_packet.get('groupingId'),
                        "code": val_ser_request_packet.get('code', ''),
                        "message": val_ser_api_resp.get('message', ''),
                    })
                    api_request_obj.extra_data = data
                    db.commit()
                    db.refresh(api_request_obj)

                response_data = {
                    "channel": val_ser_request_packet.get('channel'),
                    "hubId": val_ser_request_packet.get('hubId'),
                    "idpId": val_ser_request_packet.get('idpId'),
                    "requestId": val_ser_request_packet.get('requestId'),
                    "code": val_ser_request_packet.get('code', ''),
                    "message": val_ser_api_resp.get('message', ''),
                    "groupingId": val_ser_api_resp.get('groupingId', '')
                }
                response_csv_data.append(response_data)
            return response_csv_data
        except Exception as e:
            logger.exception(f"Exception do_validation_service_without_code :: {e}")
            return {
                **ErrorCodes.get_error_response(500)
            }


# READ CSV FOR REGISTRATION WITHOUT CODE AND FINANCE
def read_csv_reg_without_code_finance(df):
    invoice_obj = {}
    ledger_data = []
    finance_ledger_data = []
    seller_identifier_data = []
    buyer_identifier_data = []
    request_packet = []
    finance_request_packet = []
    total_row_invoices = 0
    total_invoice_request_count = 0
    seller_identifier_rows = 0
    seller_identifier_request_count = 0
    buyer_identifier_rows = 0
    buyer_identifier_request_count = 0
    error_response = {"code": "", "message": ""}
    expected_headers = SFTP_API_HEADERS.invoice_reg_without_entity_code_finance

    # actual_headers = [header.strip() for header in df.columns.tolist()]
    actual_headers = df.columns.tolist()
    extra_headers = set(expected_headers) - set(actual_headers)
    # extra_headers = set(actual_headers) - set(expected_headers)
    if extra_headers:
        error_response.update(**ErrorCodes.get_error_response(1090))
        return [error_response]
    request_packet_validation = True
    for index, row in df.iterrows():
        ledger_datas = ""
        finance_ledger_datas = ""

        if not invoice_obj:
            invoice_obj = {"requestId": str(row.requestID),
                           "sellerGst": str(row.sellerGst),
                           "buyerGst": str(row.buyerGst),
                           "channel": row.channel,
                           "hubId": row.hubId,
                           "idpId": row.idpId,
                           "groupingId": str(row.groupingId),
                           "total_invoice": row.noOfInvoices if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                           "total_buyer_identifier_request": row.buyerDataNo if row.buyerDataNo and row.buyerDataNo.isnumeric() else 0,
                           "total_buyer_identifier_count": 0,
                           "total_seller_identifier_request": row.sellerDataNo if row.sellerDataNo and row.sellerDataNo.isnumeric() else 0,
                           "total_seller_identifier_count": 0
                           }
            finance_obj = {
                "requestId": 'finance_' + str(row.requestID),
                "ledgerNo": "",
                "ledgerAmtFlag": str(row.ledgerAmtFlag),
                "lenderCategory": str(row.lenderCategory),
                "lenderName": str(row.lenderName),
                "lenderCode": str(row.lenderCode),
                "borrowerCategory": str(row.borrowerCategory)
            }
        else:
            if str(invoice_obj.get('requestId')) != str(row.requestID):
                if request_packet_validation:
                    request_packet.append(invoice_obj)
                    finance_request_packet.append(finance_obj)
                request_packet_validation = True
                invoice_obj = {
                    "requestId": str(row.requestID),
                    "sellerGst": str(row.sellerGst),
                    "buyerGst": str(row.buyerGst),
                    "channel": row.channel,
                    "hubId": row.hubId,
                    "idpId": row.idpId,
                    "groupingId": str(row.groupingId),
                    "total_invoice": row.noOfInvoices if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                    "total_buyer_identifier_request": row.buyerDataNo if row.buyerDataNo and row.buyerDataNo.isnumeric() else 0,
                    "total_buyer_identifier_count": 0,
                    "total_seller_identifier_request": row.sellerDataNo if row.sellerDataNo and row.sellerDataNo.isnumeric() else 0,
                    "total_seller_identifier_count": 0
                }

                finance_obj = {
                    "requestId": 'finance_' + str(row.requestID),
                    "ledgerNo": "",
                    "ledgerAmtFlag": str(row.ledgerAmtFlag),
                    "lenderCategory": str(row.lenderCategory),
                    "lenderName": str(row.lenderName),
                    "lenderCode": str(row.lenderCode),
                    "borrowerCategory": str(row.borrowerCategory),
                }

                ledger_data = []
                finance_ledger_data = []
                seller_identifier_data = []
                buyer_identifier_data = []
                total_row_invoices = 0
                # total_invoice_request_count = 0
                seller_identifier_rows = 0
                # seller_identifier_request_count = 0
                buyer_identifier_rows = 0
                # buyer_identifier_request_count = 0

            # request_packet.append(invoice_obj)
            # finance_request_packet.append(finance_obj)

        gst_status = row.verifyGSTNFlag
        if row.verifyGSTNFlag == str(1):
            gst_status = True
        if row.verifyGSTNFlag == str(0):
            gst_status = False

        if str(row.invoiceNo) or row.invoiceDate or str(row.invoiceAmt) or gst_status or row.invoiceDueDate:
            ledger_datas = {
                "validationType": str(row.validationType).strip() if row.validationType else "",
                "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                "invoiceNo": str(row.invoiceNo),
                "invoiceDate": row.invoiceDate,
                "invoiceAmt": str(row.invoiceAmt),
                "verifyGSTNFlag": gst_status,
                "invoiceDueDate": row.invoiceDueDate,
            }

            finance_ledger_datas = {
                "validationType": str(row.validationType).strip() if row.validationType else "",
                "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                "invoiceNo": str(row.invoiceNo),
                "financeRequestAmt": str(row.financeRequestAmt),
                "financeRequestDate": str(row.financeRequestDate),
                "dueDate": str(row.dueDate),
                "fundingAmtFlag": str(row.fundingAmtFlag),
                "adjustmentType": str(row.adjustmentType),
                "adjustmentAmt": str(row.adjustmentAmt),
                "invoiceDate": str(row.invoiceDate),
                "invoiceAmt": str(row.invoiceAmt)
            }
            total_row_invoices = total_row_invoices + 1

        if str(row.sellerIdType) or str(row.sellerIdNo) or str(row.sellerIdName) or str(row.sellerIfsc):
            seller_identifier_rows = seller_identifier_rows + 1
            invoice_obj.update({"total_seller_identifier_count": str(seller_identifier_rows)})

        seller_identifier_datas = {
            "sellerIdType": row.sellerIdType,
            "sellerIdNo": row.sellerIdNo,
            "sellerIdName": row.sellerIdName,
            "ifsc": row.sellerIfsc
        }

        if str(row.buyerIdType) or str(row.buyerIdNo) or str(row.buyerIdName) or str(row.buyerIfsc):
            buyer_identifier_rows = buyer_identifier_rows + 1
            invoice_obj.update({"total_buyer_identifier_count": str(buyer_identifier_rows)})

        buyer_identifier_datas = {
            "buyerIdType": str(row.buyerIdType),
            "buyerIdNo": str(row.buyerIdNo),
            "buyerIdName": str(row.buyerIdName),
            "ifsc": row.buyerIfsc
        }
        # if invoice_no !=
        if ledger_datas:
            ledger_data.append(ledger_datas)
            finance_ledger_data.append(finance_ledger_datas)

        if seller_identifier_datas:
            seller_identifier_data.append(seller_identifier_datas)

        if buyer_identifier_datas:
            buyer_identifier_data.append(buyer_identifier_datas)

        if ledger_datas:
            finance_obj.update({"ledgerData": finance_ledger_data})
            invoice_obj.update({
                "ledgerData": ledger_data,
                "sellerIdentifierData": seller_identifier_data,
                "buyerIdentifierData": buyer_identifier_data,
                "financeData": [finance_obj]
            })

        if (invoice_obj.get('channel').lower() != row.channel.lower().strip() or invoice_obj.get('hubId').strip()
                != row.hubId.strip() or invoice_obj.get('idpId') != row.idpId.strip() or invoice_obj.get('sellerGst')
                != row.sellerGst.strip() or invoice_obj.get('buyerGst')
                != row.buyerGst.strip()
        ):
            request_packet_validation = False

        if index == df.shape[0] - 1 and str(invoice_obj.get('requestId')) == str(row.requestID):
            if request_packet_validation:
                if invoice_obj.get('ledgerData', ''):
                    request_packet.append(invoice_obj)
                finance_request_packet.append(finance_obj)
            request_packet_validation = True

    logger.info(f"getting request packet {request_packet}")
    return request_packet


# DISBURSEMENT READ CSV
def dis_read_csv_file(df):
    invoice_obj = {}
    ledger_data = []
    request_packet = []
    total_row_invoices = 0
    total_invoice_request_count = 0
    error_response = {"code": "", "message": ""}
    expected_headers = SFTP_API_HEADERS.disbursement

    actual_headers = df.columns.tolist()
    extra_headers = set(expected_headers) - set(actual_headers)
    # extra_headers = set(actual_headers) - set(expected_headers)
    if extra_headers:
        error_response.update(**ErrorCodes.get_error_response(1090))
        return [error_response]

    # file_path = "/home/lalita/Documents/A1SFTP/3_Disbursement.csv"
    # try:
    #     df = pd.read_csv(file_path, delimiter="|", dtype=str, na_values='')
    #     df = df.replace(np.nan, '')
    #     # print(df.to_string())
    # except FileNotFoundError:
    #     raise HTTPException(status_code=404, detail="File not found")
    try:
        for index, row in df.iterrows():
            ledger_datas = ""
            logger.info(f" row.noOfInvoices {row.noOfInvoices}")

            if row.noOfInvoices:
                row.noOfInvoices = utils.cast_to_int_or_zero(row.noOfInvoices)

            # if row.channel == "":
            #     error_response.update(**ErrorCodes.get_error_response(1082))
            #     return [error_response]
            #
            # if row.hubId == "":
            #     error_response.update(**ErrorCodes.get_error_response(1081))
            #     return [error_response]
            # elif not row.hubId.isnumeric():
            #     error_response.update(**ErrorCodes.get_error_response(1085))
            #     return [error_response]
            #
            # if row.idpId == "":
            #     error_response.update(**ErrorCodes.get_error_response(1083))
            #     return [error_response]
            #
            # if row.requestID == "":
            #     error_response.update(**ErrorCodes.get_error_response(1024))
            #     return [error_response]

            # if row.noOfInvoices:
            #     if total_row_invoices and total_row_invoices != total_invoice_request_count:
            #         return {"message": "invalid no of invoices count"}
            #
            #     total_invoice_request_count = row.noOfInvoices

            if not invoice_obj:
                invoice_obj = {"requestId": str(row.requestID).strip(),
                               "ledgerNo": str(row.ledgerNo).strip(),
                               "lenderCategory": str(row.lenderCategory).strip(),
                               "lenderName": str(row.lenderName).strip(),
                               "lenderCode": str(row.lenderCode).strip(),
                               "total_invoice": row.noOfInvoices
                               }
            else:
                if str(invoice_obj.get('requestId')).strip() != str(row.requestID).strip():
                    request_packet.append(invoice_obj)
                    invoice_obj = {"requestId": str(row.requestID).strip(),
                                   "ledgerNo": str(row.ledgerNo).strip(),
                                   "lenderCategory": str(row.lenderCategory).strip(),
                                   "lenderName": str(row.lenderName).strip(),
                                   "lenderCode": str(row.lenderCode).strip(),
                                   "total_invoice": row.noOfInvoices
                                   }
                    ledger_data = []
                    total_row_invoices = 0
                    total_invoice_request_count = 0

            if index == df.shape[0] - 1 and str(invoice_obj.get('requestId')).strip() == str(row.requestID).strip():
                request_packet.append(invoice_obj)

            if (str(row.invoiceNo).strip() or str(row.disbursedFlag).strip() or str(row.disbursedAmt).strip() or str(
                    row.disbursedDate).strip()
                    or str(row.dueAmt).strip() or str(row.dueDate).strip() or str(row.invoiceDate).strip() or str(
                        row.invoiceAmt).strip()):
                # if str(row.invoiceNo).strip() or str(row.disbursedFlag).strip() or str(row.disbursedAmt).strip():
                ledger_datas = {
                    "validationType": str(row.validationType).strip() if row.validationType else "",
                    "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                    "invoiceNo": str(row.invoiceNo).strip(),
                    "disbursedFlag": str(row.disbursedFlag).strip(),
                    "disbursedAmt": str(row.disbursedAmt).strip(),
                    "disbursedDate": str(row.disbursedDate).strip(),
                    "dueAmt": str(row.dueAmt).strip(),
                    "dueDate": str(row.dueDate).strip(),
                    "invoiceDate": str(row.invoiceDate).strip(),
                    "invoiceAmt": str(row.invoiceAmt).strip(),
                }
                total_row_invoices = total_row_invoices + 1
                logger.info(f"getting ledger_datas request packet {ledger_datas}")

            if ledger_datas:
                ledger_data.append(ledger_datas)

            if ledger_datas:
                invoice_obj.update({
                    "ledgerData": ledger_data,
                    "channel": row.channel,
                    "hubId": row.hubId,
                    "idpId": row.idpId,
                })

            # if index == df.shape[0] - 1 and str(invoice_obj.get('requestId')) == str(row.requestID):
            #     if total_row_invoices and str(total_row_invoices) != total_invoice_request_count:
            #         return [{"code": 100, "message": "invalid no of invoices count"}]

    except Exception as e:
        logger.exception(f"{e}")
        raise HTTPException(status_code=404, detail="Exception found")

    logger.info(f"getting request packet {request_packet}")
    return request_packet


# REPAYMENT READ CSV
def repay_read_csv_file(df):
    invoice_obj = {}
    repayment_ledger_data = []
    request_packet = []
    total_row_invoices = 0
    total_invoice_request_count = 0
    error_response = {"code": "", "message": ""}
    expected_headers = SFTP_API_HEADERS.repayment

    actual_headers = df.columns.tolist()
    extra_headers = set(expected_headers) - set(actual_headers)
    # extra_headers = set(actual_headers) - set(expected_headers)
    if extra_headers:
        error_response.update(**ErrorCodes.get_error_response(1090))
        return [error_response]

    # file_path = "/home/lalita/Documents/A1SFTP/4 - Repayment_1.csv"
    # try:
    #     df = pd.read_csv(file_path, delimiter="|", dtype=str, na_values='')
    #     df = df.replace(np.nan, '')
    #     # print(df.to_string())
    # except FileNotFoundError:
    #     raise HTTPException(status_code=404, detail="File not found")

    for index, row in df.iterrows():
        repayment_ledger_datas = ""
        # if row.noOfInvoices:
        #     if total_row_invoices and total_row_invoices != total_invoice_request_count:
        #         return {"message": "invalid no of invoices count"}
        #
        #     total_invoice_request_count = row.noOfInvoices

        if not invoice_obj:
            invoice_obj = {"requestId": str(row.requestID).strip(),
                           "ledgerNo": str(row.ledgerNo).strip(),
                           "borrowerCategory": str(row.borrowerCategory).strip(),
                           "total_invoice": row.noOfInvoices
                           }
        else:
            if str(invoice_obj.get('requestId')).strip() != str(row.requestID).strip():
                request_packet.append(invoice_obj)
                invoice_obj = {"requestId": str(row.requestID).strip(),
                               "ledgerNo": str(row.ledgerNo).strip(),
                               "borrowerCategory": str(row.borrowerCategory).strip(),
                               "total_invoice": row.noOfInvoices
                               }
                repayment_ledger_data = []
                total_row_invoices = 0
                total_invoice_request_count = 0

        if index == df.shape[0] - 1 and str(invoice_obj.get('requestId')).strip() == str(row.requestID).strip():
            request_packet.append(invoice_obj)

        if str(row.invoiceNo).strip() or str(row.assetClassification).strip() or str(row.dueAmt).strip():
            repayment_ledger_datas = {
                "validationType": str(row.validationType).strip() if row.validationType else "",
                "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                "invoiceNo": str(row.invoiceNo).strip(),
                "assetClassification": str(row.assetClassification).strip(),
                "dueAmt": str(row.dueAmt).strip(),
                "dueDate": str(row.dueDate).strip(),
                "repaymentType": str(row.repaymentType).strip(),
                "repaymentFlag": str(row.repaymentFlag).strip(),
                "repaymentAmt": str(row.repaymentAmt).strip(),
                "repaymentDate": str(row.repaymentDate).strip(),
                "pendingDueAmt": str(row.pendingDueAmt).strip(),
                "dpd": str(row.dpd).strip(),
                "invoiceDate": str(row.invoiceDate).strip(),
                "invoiceAmt": str(row.invoiceAmt).strip()
            }
            total_row_invoices = total_row_invoices + 1
            logger.info(f"getting request packet {repayment_ledger_datas}")

        if repayment_ledger_datas:
            repayment_ledger_data.append(repayment_ledger_datas)

        if repayment_ledger_datas:
            invoice_obj.update({
                "ledgerData": repayment_ledger_data,
                "channel": str(row.channel),
                "hubId": str(row.hubId),
                "idpId": str(row.idpId),
            })

    logger.info(f"getting repayment request packet {request_packet}")
    return request_packet


# READ CSV FOR REGISTRATION WITHOUT CODE
def read_csv_reg_without_code(df):
    invoice_obj = {}
    ledger_data = []
    seller_identifier_data = []
    buyer_identifier_data = []
    request_packet = []
    total_row_invoices = 0
    seller_identifier_rows = 0
    buyer_identifier_rows = 0
    error_response = {"code": "", "message": ""}
    expected_headers = SFTP_API_HEADERS.invoice_reg_without_entity_code

    # actual_headers = [header.strip() for header in df.columns.tolist()]
    actual_headers = df.columns.tolist()
    extra_headers = set(expected_headers) - set(actual_headers)
    # extra_headers = set(actual_headers) - set(expected_headers)
    if extra_headers:
        error_response.update(**ErrorCodes.get_error_response(1090))
        return [error_response]

    request_packet_validation = True
    for index, row in df.iterrows():
        ledger_datas = ""

        if not invoice_obj:
            invoice_obj = {"requestId": str(row.requestID),
                           "sellerGst": str(row.sellerGst),
                           "buyerGst": str(row.buyerGst),
                           "channel": row.channel,
                           "hubId": row.hubId,
                           "idpId": row.idpId,
                           "groupingId": str(row.groupingId),
                           "total_invoice": row.noOfInvoices if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                           "total_buyer_identifier_request": row.buyerDataNo if row.buyerDataNo and row.buyerDataNo.isnumeric() else 0,
                           "total_buyer_identifier_count": 0,
                           "total_seller_identifier_request": row.sellerDataNo if row.sellerDataNo and row.sellerDataNo.isnumeric() else 0,
                           "total_seller_identifier_count": 0
                           }
        else:
            if str(invoice_obj.get('requestId')) != str(row.requestID):
                if request_packet_validation:
                    request_packet.append(invoice_obj)
                request_packet_validation = True
                invoice_obj = {
                    "requestId": str(row.requestID),
                    "sellerGst": str(row.sellerGst),
                    "buyerGst": str(row.buyerGst),
                    "groupingId": str(row.groupingId),
                    "channel": row.channel,
                    "hubId": row.hubId,
                    "idpId": row.idpId,
                    "total_invoice": row.noOfInvoices if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                    "total_buyer_identifier_request": row.buyerDataNo if row.buyerDataNo and row.buyerDataNo.isnumeric() else 0,
                    "total_buyer_identifier_count": 0,
                    "total_seller_identifier_request": row.sellerDataNo if row.sellerDataNo and row.sellerDataNo.isnumeric() else 0,
                    "total_seller_identifier_count": 0
                }

                ledger_data = []
                seller_identifier_data = []
                buyer_identifier_data = []
                total_row_invoices = 0
                seller_identifier_rows = 0
                buyer_identifier_rows = 0

        gst_status = row.verifyGSTNFlag
        if row.verifyGSTNFlag == str(1):
            gst_status = True
        if row.verifyGSTNFlag == str(0):
            gst_status = False

        if str(row.invoiceNo) or row.invoiceDate or str(row.invoiceAmt) or gst_status or row.invoiceDueDate:
            ledger_datas = {
                "validationType": str(row.validationType).strip() if row.validationType else "",
                "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                "invoiceNo": str(row.invoiceNo),
                "invoiceDate": row.invoiceDate,
                "invoiceAmt": str(row.invoiceAmt),
                "verifyGSTNFlag": gst_status,
                "invoiceDueDate": row.invoiceDueDate,
            }

            total_row_invoices = total_row_invoices + 1

        if str(row.sellerIdType) or str(row.sellerIdNo) or str(row.sellerIdName) or str(row.sellerIfsc):
            seller_identifier_rows = seller_identifier_rows + 1
            invoice_obj.update({"total_seller_identifier_count": str(seller_identifier_rows)})

        seller_identifier_datas = {
            "sellerIdType": row.sellerIdType,
            "sellerIdNo": row.sellerIdNo,
            "sellerIdName": row.sellerIdName,
            "ifsc": row.sellerIfsc
        }

        if str(row.buyerIdType) or str(row.buyerIdNo) or str(row.buyerIdName) or str(row.buyerIfsc):
            buyer_identifier_rows = buyer_identifier_rows + 1
            invoice_obj.update({"total_buyer_identifier_count": str(buyer_identifier_rows)})

        buyer_identifier_datas = {
            "buyerIdType": str(row.buyerIdType),
            "buyerIdNo": str(row.buyerIdNo),
            "buyerIdName": str(row.buyerIdName),
            "ifsc": row.buyerIfsc
        }
        # if invoice_no !=
        if ledger_datas:
            ledger_data.append(ledger_datas)

        if seller_identifier_datas:
            seller_identifier_data.append(seller_identifier_datas)

        if buyer_identifier_datas:
            buyer_identifier_data.append(buyer_identifier_datas)

        if ledger_datas:
            invoice_obj.update({
                "ledgerData": ledger_data,
                "sellerIdentifierData": seller_identifier_data,
                "buyerIdentifierData": buyer_identifier_data
            })

        if (invoice_obj.get('channel').lower() != row.channel.lower().strip() or invoice_obj.get('hubId').strip()
                != row.hubId.strip() or invoice_obj.get('idpId') != row.idpId.strip() or invoice_obj.get('sellerGst')
                != row.sellerGst.strip() or invoice_obj.get('buyerGst')
                != row.buyerGst.strip()
        ):
            request_packet_validation = False

        if index == df.shape[0] - 1 and str(invoice_obj.get('requestId')) == str(row.requestID):
            if request_packet_validation:
                if invoice_obj.get('ledgerData'):
                    request_packet.append(invoice_obj)
            request_packet_validation = True

    logger.info(f"getting request packet {request_packet}")
    return request_packet


# READ CSV FOR REGISTRATION WITH CODE
def read_csv_reg_with_code(df):
    invoice_obj = {}
    ledger_data = []
    request_packet = []
    total_row_invoices = 0
    error_response = {"code": "", "message": ""}
    expected_headers = SFTP_API_HEADERS.invoice_reg_with_entity_code

    # actual_headers = [header.strip() for header in df.columns.tolist()]
    actual_headers = df.columns.tolist()
    extra_headers = set(expected_headers) - set(actual_headers)
    # extra_headers = set(actual_headers) - set(expected_headers)
    if extra_headers:
        error_response.update(**ErrorCodes.get_error_response(1090))
        return [error_response]

    hub_validation = True
    for index, row in df.iterrows():
        ledger_datas = ""
        if not invoice_obj:
            invoice_obj = {"requestId": str(row.requestID).strip(),
                           "sellerCode": str(row.sellerCode).strip(),
                           "buyerCode": str(row.buyerCode).strip(),
                           "sellerGst": str(row.sellerGst).strip(),
                           "buyerGst": str(row.buyerGst).strip(),
                           "groupingId": str(row.groupingId).strip(),
                           "total_invoice": row.noOfInvoices.strip() if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                           "channel": row.channel.strip(),
                           "hubId": row.hubId.strip(),
                           "idpId": row.idpId.strip()
                           }
        else:
            if str(invoice_obj.get('requestId')) != str(row.requestID).strip():
                if hub_validation:
                    request_packet.append(invoice_obj)
                hub_validation = True

                invoice_obj = {
                    "requestId": str(row.requestID).strip(),
                    "sellerCode": str(row.sellerCode).strip(),
                    "buyerCode": str(row.buyerCode).strip(),
                    "sellerGst": str(row.sellerGst).strip(),
                    "buyerGst": str(row.buyerGst).strip(),
                    "groupingId": str(row.groupingId).strip(),
                    "total_invoice": row.noOfInvoices.strip() if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                    "channel": row.channel.strip(),
                    "hubId": row.hubId.strip(),
                    "idpId": row.idpId.strip()
                }

                ledger_data = []
                total_row_invoices = 0

        gst_status = row.verifyGSTNFlag
        if row.verifyGSTNFlag == str(1):
            gst_status = True
        if row.verifyGSTNFlag == str(0):
            gst_status = False

        if str(row.invoiceNo).strip() or row.invoiceDate.strip() or str(
                row.invoiceAmt).strip() or gst_status or row.invoiceDueDate.strip():
            ledger_datas = {
                "validationType": str(row.validationType).strip() if row.validationType else "",
                "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                "invoiceNo": str(row.invoiceNo).strip(),
                "invoiceDate": row.invoiceDate.strip(),
                "invoiceAmt": str(row.invoiceAmt).strip(),
                "verifyGSTNFlag": gst_status,
                "invoiceDueDate": row.invoiceDueDate.strip(),
            }

            total_row_invoices = total_row_invoices + 1

        if ledger_datas:
            ledger_data.append(ledger_datas)

        if ledger_datas:
            invoice_obj.update({
                "ledgerData": ledger_data
            })
            if (invoice_obj.get('channel').lower() != row.channel.lower().strip() or invoice_obj.get(
                    'hubId').strip() != row.hubId.strip() or invoice_obj.get(
                'idpId') != row.idpId.strip() or invoice_obj.get('sellerCode').strip() != row.sellerCode.strip()
                    or invoice_obj.get('buyerCode').strip() != row.buyerCode.strip() or
                    invoice_obj.get('sellerGst').strip() != row.sellerGst.strip() or invoice_obj.get(
                        'buyerGst').strip() != row.buyerGst.strip()):
                hub_validation = False

            if index == df.shape[0] - 1 and str(invoice_obj.get('requestId')) == str(row.requestID):
                if hub_validation:
                    request_packet.append(invoice_obj)
                hub_validation = True

    logger.info(f"getting request packet {request_packet}")
    return request_packet


# READ CSV FOR REGISTRATION WITH CODE AND FINANCE
def read_csv_reg_with_code_finance(df):
    invoice_obj = {}
    ledger_data = []
    finance_ledger_data = []
    request_packet = []
    finance_request_packet = []
    total_row_invoices = 0
    error_response = {"code": "", "message": ""}
    expected_headers = SFTP_API_HEADERS.invoice_reg_with_entity_code_finance

    # actual_headers = [header.strip() for header in df.columns.tolist()]
    actual_headers = df.columns.tolist()

    extra_headers = set(expected_headers) - set(actual_headers)
    # extra_headers = set(actual_headers) - set(expected_headers)
    if extra_headers:
        error_response.update(**ErrorCodes.get_error_response(1090))
        return [error_response]

    hub_validation = True
    for index, row in df.iterrows():
        ledger_datas = ""
        finance_ledger_datas = ""

        if not invoice_obj:
            invoice_obj = {"requestId": str(row.requestID).strip(),
                           "sellerCode": str(row.sellerCode).strip(),
                           "buyerCode": str(row.buyerCode).strip(),
                           "sellerGst": str(row.sellerGst).strip(),
                           "buyerGst": str(row.buyerGst).strip(),
                           "groupingId": str(row.groupingId).strip(),
                           "total_invoice": row.noOfInvoices.strip() if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                           "channel": row.channel.strip(),
                           "hubId": row.hubId.strip(),
                           "idpId": row.idpId.strip()
                           }
            finance_obj = {
                "requestId": 'finance_' + str(row.requestID).strip(),
                "ledgerNo": "",
                "ledgerAmtFlag": str(row.ledgerAmtFlag).strip(),
                "lenderCategory": str(row.lenderCategory).strip(),
                "lenderName": str(row.lenderName).strip(),
                "lenderCode": str(row.lenderCode).strip(),
                "borrowerCategory": str(row.borrowerCategory).strip()
            }
        else:
            if str(invoice_obj.get('requestId')) != str(row.requestID).strip():
                if hub_validation:
                    request_packet.append(invoice_obj)
                    finance_request_packet.append(finance_obj)
                hub_validation = True
                invoice_obj = {"requestId": str(row.requestID).strip(),
                               "sellerCode": str(row.sellerCode).strip(),
                               "buyerCode": str(row.buyerCode).strip(),
                               "sellerGst": str(row.sellerGst).strip(),
                               "buyerGst": str(row.buyerGst).strip(),
                               "groupingId": str(row.groupingId).strip(),
                               "total_invoice": row.noOfInvoices.strip() if row.noOfInvoices and row.noOfInvoices.isnumeric() else 0,
                               "channel": row.channel.strip(),
                               "hubId": row.hubId.strip(),
                               "idpId": row.idpId.strip()
                               }

                finance_obj = {
                    "requestId": 'finance_' + str(row.requestID).strip(),
                    "ledgerNo": "",
                    "ledgerAmtFlag": str(row.ledgerAmtFlag).strip(),
                    "lenderCategory": str(row.lenderCategory).strip(),
                    "lenderName": str(row.lenderName).strip(),
                    "lenderCode": str(row.lenderCode).strip(),
                    "borrowerCategory": str(row.borrowerCategory).strip(),
                }

                ledger_data = []
                finance_ledger_data = []
                total_row_invoices = 0

        gst_status = row.verifyGSTNFlag.strip()
        if row.verifyGSTNFlag == str(1):
            gst_status = True
        if row.verifyGSTNFlag == str(0):
            gst_status = False

        if str(row.invoiceNo).strip() or row.invoiceDate.strip() or str(
                row.invoiceAmt).strip() or gst_status or row.invoiceDueDate.strip():
            ledger_datas = {
                "validationType": str(row.validationType).strip() if row.validationType else "",
                "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                "invoiceNo": str(row.invoiceNo).strip(),
                "invoiceDate": row.invoiceDate.strip(),
                "invoiceAmt": str(row.invoiceAmt).strip(),
                "verifyGSTNFlag": gst_status,
                "invoiceDueDate": row.invoiceDueDate.strip(),
            }

            finance_ledger_datas = {
                "validationType": str(row.validationType).strip() if row.validationType else "",
                "validationRefNo": str(row.validationRefNo).strip() if row.validationRefNo else "",
                "invoiceNo": str(row.invoiceNo).strip(),
                "financeRequestAmt": str(row.financeRequestAmt).strip(),
                "financeRequestDate": str(row.financeRequestDate).strip(),
                "dueDate": str(row.dueDate).strip(),
                "fundingAmtFlag": str(row.fundingAmtFlag).strip(),
                "adjustmentType": str(row.adjustmentType).strip(),
                "adjustmentAmt": str(row.adjustmentAmt).strip(),
                "invoiceDate": str(row.invoiceDate).strip(),
                "invoiceAmt": str(row.invoiceAmt).strip()
            }
            total_row_invoices = total_row_invoices + 1

        if ledger_datas:
            ledger_data.append(ledger_datas)
            finance_ledger_data.append(finance_ledger_datas)

        if ledger_datas:
            finance_obj.update({"ledgerData": finance_ledger_data})
            invoice_obj.update({
                "ledgerData": ledger_data,
                "financeData": [finance_obj]
            })
            if (invoice_obj.get('channel').lower() != row.channel.lower().strip() or invoice_obj.get(
                    'hubId').strip() != row.hubId.strip() or invoice_obj.get(
                'idpId') != row.idpId.strip() or invoice_obj.get('sellerCode').strip() != row.sellerCode.strip()
                    or invoice_obj.get('buyerCode').strip() != row.buyerCode.strip() or
                    invoice_obj.get('sellerGst').strip() != row.sellerGst.strip() or invoice_obj.get(
                        'buyerGst').strip() != row.buyerGst.strip()):
                hub_validation = False

            if index == df.shape[0] - 1 and str(invoice_obj.get('requestId')) == str(row.requestID).strip():
                if hub_validation:
                    request_packet.append(invoice_obj)
                    finance_request_packet.append(finance_obj)
                hub_validation = True

    logger.info(f"getting request packet {request_packet}")
    return request_packet


def read_csv_entity_reg(df):
    entity_obj = {}
    entity_register_data = {}
    request_packet = []
    entity_code = ""
    no_of_entity = 0
    no_of_identifiers = 0
    error_response = {"code": "", "message": ""}
    expected_headers = SFTP_API_HEADERS.entity_registration

    actual_headers = df.columns.tolist()
    extra_headers = set(expected_headers) - set(actual_headers)
    # extra_headers = set(actual_headers) - set(expected_headers)
    if extra_headers:
        error_response.update(**ErrorCodes.get_error_response(1090))
        return [error_response]

    request_packet_validation = True
    for index, row in df.iterrows():
        if entity_code != row.entityCode:
            if entity_code and entity_code != str(row.entityCode).strip() or len(entity_obj) != 0:
                entity_obj.update({'totalEntityCount': no_of_entity})
                entity_obj.get('entityRegisterData').append(entity_register_data)

            entity_code = str(row.entityCode).strip()

        if not entity_obj:
            entity_obj = {
                "requestId": str(row.requestID).strip(),
                "channel": str(row.channel).strip(),
                "hubId": str(row.hubId).strip(),
                "idpId": str(row.idpId).strip(),
                "totalEntityRequest": row.noOfEntities if row.noOfEntities and row.noOfEntities.isnumeric() else "0",
                "totalIdentifiersRequest": row.noOfIdentifiers if row.noOfIdentifiers else "0",
                "totalEntityCount": 0,
                "totalIdentifiersCount": 0,
                "entityRegisterData": []
            }
        else:
            if str(entity_obj.get('requestId')) != str(row.requestID).strip():
                if request_packet_validation:
                    request_packet.append(entity_obj)
                request_packet_validation = True
                entity_obj = {
                    "requestId": str(row.requestID).strip(),
                    "channel": str(row.channel).strip(),
                    "hubId": str(row.hubId).strip(),
                    "idpId": str(row.idpId).strip(),
                    "totalEntityRequest": row.noOfEntities if row.noOfEntities and row.noOfEntities.isnumeric() else "0",
                    "totalIdentifiersRequest": row.noOfIdentifiers if row.noOfIdentifiers else "0",
                    "totalEntityCount": 0,
                    "totalIdentifiersCount": 0,
                    "entityRegisterData": []
                }
                entity_register_data = {}
                no_of_entity = 0
                no_of_identifiers = 0

        if str(row.entityCode).strip() and entity_register_data.get('entityCode', '') != str(
                row.entityCode).strip() or len(entity_register_data) == 0:
            entity_register_data = {
                "entityCode": str(row.entityCode).strip(),
                "totalIdentifiersRequest": 0,
                "entityIdentifierData": []
            }
            no_of_entity = no_of_entity + 1
            identifier_request = row.noOfIdentifiers if row.noOfIdentifiers and row.noOfIdentifiers.isnumeric() else 0
            total_identifier = entity_register_data.get('totalIdentifiersRequest') + int(identifier_request)
            entity_register_data.update({"totalIdentifiersRequest": total_identifier})

        if str(row.entityIdType).strip() or str(row.entityIdNo).strip() or str(row.entityIdName).strip() or str(
                row.ifsc):
            entity_identifier_data = {
                "entityIdType": str(row.entityIdType).strip(),
                "entityIdNo": str(row.entityIdNo).strip(),
                "entityIdName": str(row.entityIdName).strip(),
                "ifsc": str(row.ifsc).strip() if str(row.ifsc) else ""
            }
            entity_register_data.get('entityIdentifierData').append(entity_identifier_data)
            no_of_identifiers = no_of_identifiers + 1
            entity_obj.update({'totalIdentifiersCount': no_of_identifiers})

        if (entity_obj.get('channel').lower() != row.channel.lower().strip() or entity_obj.get('hubId').strip() !=
                row.hubId.strip() or entity_obj.get('idpId') != row.idpId.strip()):
            request_packet_validation = False

        if index == df.shape[0] - 1 and str(entity_obj.get('requestId')) == str(row.requestID).strip():
            if request_packet_validation:
                entity_obj.update({'totalEntityCount': no_of_entity})
                entity_obj.get('entityRegisterData').append(entity_register_data)
                request_packet.append(entity_obj)

            request_packet_validation = True

    logger.info(f"getting request packet {request_packet}")
    return request_packet


# CANCELLATION READ CSV
def cancel_read_csv_file(df):
    invoice_obj = {}
    request_packet = []

    error_response = {"code": "", "message": ""}
    expected_headers = SFTP_API_HEADERS.cancel

    actual_headers = df.columns.tolist()
    extra_headers = set(expected_headers) - set(actual_headers)
    # extra_headers = set(actual_headers) - set(expected_headers)
    if extra_headers:
        error_response.update(**ErrorCodes.get_error_response(1090))
        return [error_response]
    logger.info(f"{df} :: \n {df.iterrows()}")
    for index, row in df.iterrows():
        print(f"indside loop {index}")
        invoice_obj = {
            "channel": str(row.channel).strip(),
            "hubId": str(row.hubId).strip(),
            "idpId": str(row.idpId).strip(),
            "requestId": str(row.requestID).strip(),
            "ledgerNo": str(row.ledgerNo).strip(),
            "cancellationReason": str(row.cancellationReason).strip(),
        }
        request_packet.append(invoice_obj)
        #if not invoice_obj:
        #    invoice_obj = {
        #                    "channel": str(row.channel).strip(),
        #                    "hubId": str(row.hubId).strip(),
        #                    "idpId": str(row.idpId).strip(),
        #                    "requestId": str(row.requestID).strip(),
        #                   "ledgerNo": str(row.ledgerNo).strip(),
        #                   "cancellationReason": str(row.cancellationReason).strip(),
        #                   }
        #else:
        #    if str(invoice_obj.get('requestId')).strip() != str(row.requestID).strip():
        #        request_packet.append(invoice_obj)
        #        invoice_obj = {
        #            "channel": row.channel,
        #            "hubId": row.hubId,
        #            "idpId": row.idpId,
        #            "requestId": str(row.requestID).strip(),
        #           "ledgerNo": str(row.ledgerNo).strip(),
        #           "cancellationReason": str(row.cancellationReason).strip(),
        #           }
        logger.info(f"fsdvfdsvsdvsd:  {index} {df.shape[0]}")
        #if index <= df.shape[0] - 1:
        #    logger.info("Inside if canceleeeeeeee")
        #    request_packet.append(invoice_obj)

        # invoice_obj.update({
        #     "channel": row.channel,
        #     "hubId": row.hubId,
        #     "idpId": row.idpId,
        # })
        # logger.info(f" cancel data -------{invoice_obj}")

    logger.info(f"getting cancellation request packet {request_packet}")
    return request_packet


# REGISTRATION WITHOUT CODE AND FINANCE RETURN API CALL RESPONSE
def do_reg_without_code_fin(registration_response):
    db = next(get_db())
    from routers.registration import sync_registration_without_code
    from routers.finance import sync_finance, sync_disbursement
    try:
        logger.info(f"getting registration_response data {registration_response}")
        response_data = {
            "channel": "",
            "hubId": "",
            "idpId": "",
            "requestID": "",
            "groupingId": "",
            "APIName": "Invoice Reg Without EC",
            "ledgerNo": "",
            "code": "500",
            "message": "Incorrect data",
            "invoiceNo": "",
            "invoiceDate": "",
            "invoiceAmt": "",
            "invoiceStatus": "",
            "fundedAmt": "",
            "gstVerificationStatus": ""
        }
        response_csv_data = []
        if registration_response and not registration_response[0].get('code', ''):
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

                validate_column_response = validate_column(reg_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                logger.info(f"getting request id {reg_request_packet.get('requestId')}")
                if merchant_details:
                    finance_data = reg_request_packet.get('financeData')
                    reg_request_packet.pop('channel')
                    reg_request_packet.pop('hubId')
                    reg_request_packet.pop('idpId')
                    reg_request_packet.pop('total_buyer_identifier_request')
                    reg_request_packet.pop('total_buyer_identifier_count')
                    reg_request_packet.pop('total_seller_identifier_request')
                    reg_request_packet.pop('total_seller_identifier_count')
                    reg_request_packet.pop('total_invoice')
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
                                    finance_pay_load_json = AsyncBulkFinanceSchema(**finance_request_packet)
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
                                                "gstVerificationStatus": finance_response.get(
                                                    'gstVerificationStatus')
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
        else:
            logger.info(f"getting error of csv reading ########## {registration_response}")
            if registration_response:
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

        return response_csv_data

    except Exception as e:
        logger.info("Exception occurs while download error files from xms server {}".format(e))
        return False


# REGISTRATION WITHOUT CODE RETURN API CALL RESPONSE
def do_reg_without_code(registration_response):
    db = next(get_db())
    from routers.registration import sync_registration_without_code
    try:
        logger.info(f"getting registration_response data {registration_response}")
        response_data = {
            "channel": "",
            "hubId": "",
            "idpId": "",
            "requestID": "",
            "groupingId": "",
            "APIName": "Invoice Reg Without EC",
            "ledgerNo": "",
            "code": "500",
            "message": "Incorrect data"
        }
        response_csv_data = []
        if registration_response and not registration_response[0].get('code', ''):
            # if not registration_response[0].get('code', ''):
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
                    "message": ""
                }

                validate_column_response = validate_column(reg_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                logger.info(f"getting request id {reg_request_packet.get('requestId')}")
                if merchant_details:
                    reg_request_packet.pop('channel')
                    reg_request_packet.pop('hubId')
                    reg_request_packet.pop('idpId')
                    reg_request_packet.pop('total_buyer_identifier_request')
                    reg_request_packet.pop('total_buyer_identifier_count')
                    reg_request_packet.pop('total_seller_identifier_request')
                    reg_request_packet.pop('total_seller_identifier_count')
                    reg_request_packet.pop('total_invoice')
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
                    else:
                        logger.info(f"getting error of registration schema ########## {pay_load}")
                        response_data.update({"code": "", "message": pay_load.get('error')})
                        response_csv_data.append(response_data)
        else:
            logger.info(f"getting error of csv reading ########## {registration_response}")
            if registration_response:
                response_data = {
                    "channel": '',
                    "hubId": '',
                    "idpId": '',
                    "requestID": '',
                    "groupingId": '',
                    "APIName": "Invoice Reg Without EC",
                    "ledgerNo": "",
                    "code": registration_response[0].get('code', ''),
                    "message": registration_response[0].get('message', '')
                }
            response_csv_data.append(response_data)

        return response_csv_data

    except Exception as e:
        logger.info("Exception occurs while download error files from xms server {}".format(e))
        return False


# DISBURSEMENT API CALL RESPONSE
def do_disbursement(disbursement_response):
    db = next(get_db())
    from routers.finance import sync_disbursement
    try:
        logger.info(f"disbursement_response<<<<<<->>>>>>>>>>>{disbursement_response}")
        response_csv_data = []
        if disbursement_response[0].get('code', ''):
            logger.info(
                f"do_disbursement :: getting error of csv reading ########## {disbursement_response}")
            response_data = {
                "channel": '',
                "hubId": '',
                "idpId": '',
                "requestID": '',
                "groupingId": '',
                "ledgerNo": "",
                "code": disbursement_response[0].get('code', ''),
                "message": disbursement_response[0].get('message', ''),
            }
            response_csv_data.append(response_data)
            return response_csv_data

        for disb_request_packet in disbursement_response:
            logger.info(f"getting registration request packet {disb_request_packet}")
            # merchant_details = db.query(MerchantDetails).filter(
            #     MerchantDetails.unique_id == disb_request_packet.get('idpId')).first()
            response_data = {
                "channel": disb_request_packet.get('channel'),
                "hubId": disb_request_packet.get('hubId'),
                "idpId": disb_request_packet.get('idpId'),
                "requestID": disb_request_packet.get('requestId'),
                "ledgerNo": disb_request_packet.get('ledgerNo'),
                "code": "",
                "message": ""
            }
            # logger.info(f"getting request id before channel {disb_request_packet.get('requestId')}")
            # if disb_request_packet.get('channel') == "":
            #     response_data.update(**ErrorCodes.get_error_response(1082))
            #     response_csv_data.append(response_data)
            #     continue

            # logger.info(f"getting request id before len channel {disb_request_packet.get('requestId')}")
            # if len(disb_request_packet.get('channel')) > 16:
            #     response_data.update(**ErrorCodes.get_error_response(1086))
            #     response_csv_data.append(response_data)
            #     continue

            # if not special_char_pattern.match(disb_request_packet.get('channel')):
            #     response_data.update(**ErrorCodes.get_error_response(1091))
            #     response_csv_data.append(response_data)
            #     continue

            # if not disb_request_packet.get('channel', '') in ["IDP", "HUB", "IBDIC"]:
            #     response_data.update(**ErrorCodes.get_error_response(1092))
            #     response_csv_data.append(response_data)
            #     continue

            # logger.info(f"getting request id before hubid {disb_request_packet.get('requestId')}")
            # if disb_request_packet.get('hubId') == "":
            #     response_data.update(**ErrorCodes.get_error_response(1081))
            #     response_csv_data.append(response_data)
            #     continue
            # elif not disb_request_packet.get('hubId').isnumeric():
            #     response_data.update(**ErrorCodes.get_error_response(1085))
            #     response_csv_data.append(response_data)
            #     continue

            # logger.info(f"getting request id before idpid {disb_request_packet.get('requestId')}")
            # if disb_request_packet.get('idpId') == "":
            #     response_data.update(**ErrorCodes.get_error_response(1083))
            #     response_csv_data.append(response_data)
            #     continue

            # logger.info(f"getting request id before ledgerData {disb_request_packet.get('requestId')}")
            # if str(len(disb_request_packet.get('ledgerData'))) != str(disb_request_packet.get('total_invoice')):
            #     response_data.update(**ErrorCodes.get_error_response(1087))
            #     response_csv_data.append(response_data)
            #     continue

            mer_unique_id = disb_request_packet.get('idpId', '')
            merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()
            # if not merchant_details:
            #     response_data.update(**ErrorCodes.get_error_response(1002))
            #     response_csv_data.append(response_data)
            #     continue
            # else:
            #     hub_details = db.query(Hub).filter(
            #         Hub.id == merchant_details.hub_id).first()
            #     if not hub_details or hub_details.unique_id != disb_request_packet.get('hubId'):
            #         response_data.update(**ErrorCodes.get_error_response(1084))
            #         response_csv_data.append(response_data)
            #         continue

            validate_column_response = validate_column(disb_request_packet, merchant_details, db)
            if validate_column_response.get('code') != 200:
                response_data.update(validate_column_response)
                response_csv_data.append(response_data)
                continue

            merchant_key = merchant_details.merchant_key

            logger.info(f"getting request id {disb_request_packet.get('requestId')}")

            # if merchant_details:
            disb_request_packet.pop('total_invoice')

            disb_request_packet_copy = copy.deepcopy(disb_request_packet)
            disb_request_packet_copy.pop("channel")
            disb_request_packet_copy.pop("hubId")
            disb_request_packet_copy.pop("idpId")
            dis_signature = utils.create_signature(disb_request_packet_copy, merchant_details.merchant_secret)
            disb_request_packet.update({"signature": dis_signature})

            logger.info(f"getting Disbursement request packet {disb_request_packet}")
            try:
                dis_pay_load = AsyncDisburseSchema(**disb_request_packet)
            except HTTPException as httpExc:
                logger.info(f"httpExc:: {httpExc.detail}")
                dis_pay_load = {"code": 400, "error": str(httpExc.detail)}
            except Exception as e:
                logger.info(f" Exception:: {e}")
                dis_pay_load = {"code": 500, "error": str(e)}

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

        field_name = [
            "channel",
            "hubId",
            "idpId",
            "requestID",
            "ledgerNo",
            "code",
            "message"
        ]
        return response_csv_data
    except Exception as e:
        logger.exception(f"Exception {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }


# REPAYMENT API CALL RESPONSE
def do_repayment(repayment_response):
    db = next(get_db())
    from routers.finance import sync_repayment
    # logger.info(f"getting merchant_key >>>>>>>>>>>>>> {merchant_key}")
    # merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
    # repayment_response = utils.repay_read_csv_file()
    # api_url = 'http://127.0.0.1:8000/syncDisbursement/bY8ziYU7OnHCLkrQRJdWPA'
    try:
        logger.info(f"repayment_response<<<<<<->>>>>>>>>>>{repayment_response}")
        response_csv_data = []
        if repayment_response[0].get('code', ''):
            logger.info(
                f"do_repayment :: getting error of csv reading ########## {repayment_response}")
            response_data = {
                "channel": '',
                "hubId": '',
                "idpId": '',
                "requestID": '',
                "groupingId": '',
                "ledgerNo": "",
                "code": repayment_response[0].get('code', ''),
                "message": repayment_response[0].get('message', ''),
            }
            response_csv_data.append(response_data)
            return response_csv_data

        for repay_request_packet in repayment_response:
            logger.info(f"getting registration request packet {repay_request_packet}")
            response_data = {
                "channel": repay_request_packet.get('channel'),
                "hubId": repay_request_packet.get('hubId'),
                "idpId": repay_request_packet.get('idpId'),
                "requestID": repay_request_packet.get('requestId'),
                "ledgerNo": repay_request_packet.get('ledgerNo'),
                "code": "",
                "message": ""
            }
            # logger.info(f"getting request id before channel {repay_request_packet.get('requestId')}")
            # if repay_request_packet.get('channel') == "":
            #     response_data.update(**ErrorCodes.get_error_response(1082))
            #     response_csv_data.append(response_data)
            #     continue

            # logger.info(f"getting request id before len channel {repay_request_packet.get('requestId')}")
            # if len(repay_request_packet.get('channel')) > 16:
            #     response_data.update(**ErrorCodes.get_error_response(1086))
            #     response_csv_data.append(response_data)
            #     continue

            # if not special_char_pattern.match(repay_request_packet.get('channel')):
            #     response_data.update(**ErrorCodes.get_error_response(1091))
            #     response_csv_data.append(response_data)
            #     continue

            # if not repay_request_packet.get('channel', '') in ["IDP", "HUB", "IBDIC"]:
            #     response_data.update(**ErrorCodes.get_error_response(1092))
            #     response_csv_data.append(response_data)
            #     continue

            # logger.info(f"getting request id before hubid {repay_request_packet.get('requestId')}")
            # if repay_request_packet.get('hubId') == "":
            #     response_data.update(**ErrorCodes.get_error_response(1081))
            #     response_csv_data.append(response_data)
            #     continue
            # elif not repay_request_packet.get('hubId').isnumeric():
            #     response_data.update(**ErrorCodes.get_error_response(1085))
            #     response_csv_data.append(response_data)
            #     continue

            # logger.info(f"getting request id before idpid {repay_request_packet.get('requestId')}")
            # if repay_request_packet.get('idpId') == "":
            #     response_data.update(**ErrorCodes.get_error_response(1083))
            #     response_csv_data.append(response_data)
            #     continue

            # logger.info(f"getting request id before ledgerData {repay_request_packet.get('requestId')}")
            # if str(len(repay_request_packet.get('ledgerData'))) != str(repay_request_packet.get('total_invoice')):
            #     response_data.update(**ErrorCodes.get_error_response(1087))
            #     response_csv_data.append(response_data)
            #     continue

            mer_unique_id = repay_request_packet.get('idpId', '')
            merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()
            # if not merchant_details:
            #     response_data.update(**ErrorCodes.get_error_response(1002))
            #     response_csv_data.append(response_data)
            #     continue
            # else:
            #     hub_details = db.query(Hub).filter(
            #         Hub.id == merchant_details.hub_id).first()
            #     if not hub_details or hub_details.unique_id != repay_request_packet.get('hubId'):
            #         response_data.update(**ErrorCodes.get_error_response(1084))
            #         response_csv_data.append(response_data)
            #         continue
            validate_column_response = validate_column(repay_request_packet, merchant_details, db)
            if validate_column_response.get('code') != 200:
                response_data.update(validate_column_response)
                response_csv_data.append(response_data)
                continue

            merchant_key = merchant_details.merchant_key

            repay_request_packet.pop('total_invoice')

            logger.info(f"getting repayment request packet {repay_request_packet}")
            repay_request_packet_copy = copy.deepcopy(repay_request_packet)
            repay_request_packet_copy.pop("channel")
            repay_request_packet_copy.pop("hubId")
            repay_request_packet_copy.pop("idpId")
            repay_signature = utils.create_signature(repay_request_packet_copy, merchant_details.merchant_secret)
            repay_request_packet.update({"signature": repay_signature})
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
        return response_csv_data
    except Exception as e:
        logger.exception(f"Exception {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }


# CANCELLATION API CALL RESPONSE
def do_cancellation(cancellation_response):
    db = next(get_db())
    from routers.enquiry import cancel_fund_ledger
    try:
        logger.info(f"cancellation_response<<<<<<->>>>>>>>>>>{cancellation_response}")
        response_csv_data = []
        if cancellation_response[0].get('code', ''):
            logger.info(
                f"do_cancellation :: getting error of csv reading ########## {cancellation_response}")
            response_data = {
                "channel": '',
                "hubId": '',
                "idpId": '',
                "requestID": '',
                "ledgerNo": "",
                "code": cancellation_response[0].get('code', ''),
                "message": cancellation_response[0].get('message', ''),
            }
            response_csv_data.append(response_data)
            return response_csv_data

        for cancel_request_packet in cancellation_response:
            logger.info(f"getting registration request packet {cancel_request_packet}")

            response_data = {
                "channel": cancel_request_packet.get('channel'),
                "hubId": cancel_request_packet.get('hubId'),
                "idpId": cancel_request_packet.get('idpId'),
                "requestID": cancel_request_packet.get('requestId'),
                "ledgerNo": cancel_request_packet.get('ledgerNo'),
                "code": "",
                "message": ""
            }

            mer_unique_id = cancel_request_packet.get('idpId', '')
            merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == mer_unique_id).first()

            validate_column_response = validate_column(cancel_request_packet, merchant_details, db)
            if validate_column_response.get('code') != 200:
                response_data.update(validate_column_response)
                response_csv_data.append(response_data)
                continue

            merchant_key = merchant_details.merchant_key

            logger.info(f"getting request id {cancel_request_packet.get('requestId')}")

            # if merchant_details:
            # cancel_request_packet.pop('total_invoice')

            cancel_request_packet_copy = copy.deepcopy(cancel_request_packet)
            cancel_request_packet_copy.pop("channel")
            cancel_request_packet_copy.pop("hubId")
            cancel_request_packet_copy.pop("idpId")
            cancel_signature = utils.create_signature(cancel_request_packet_copy, merchant_details.merchant_secret)
            cancel_request_packet.update({"signature": cancel_signature})

            logger.info(f"getting Cancellation request packet {cancel_request_packet}")
            try:
                cancel_pay_load = CancelLedgerSchema(**cancel_request_packet)
            except HTTPException as httpExc:
                logger.info(f"httpExc:: {httpExc.detail}")
                cancel_pay_load = {"code": 400, "error": str(httpExc.detail)}
            except Exception as e:
                logger.info(f" Exception:: {e}")
                cancel_pay_load = {"code": 500, "error": str(e)}

            if isinstance(cancel_pay_load, dict) and cancel_pay_load.get('error'):
                response_data.update({"code": cancel_pay_load.get('code'), "message": cancel_pay_load.get('error')})
                response_csv_data.append(response_data)
                continue

            cancel_api_response = cancel_fund_ledger(merchant_key, cancel_pay_load, db)
            logger.info(f"cancellation response:: {cancel_api_response}")
            response_data.update({
                "code": cancel_api_response.get('code'),
                "message": cancel_api_response.get('message')
            })
            response_csv_data.append(response_data)

        field_name = [
            "channel",
            "hubId",
            "idpId",
            "requestID",
            "ledgerNo",
            "code",
            "message"
        ]
        return response_csv_data
    except Exception as e:
        logger.exception(f"Exception {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }


# REGISTRATION WITH CODE RETURN API CALL RESPONSE
def do_reg_with_code(registration_response):
    db = next(get_db())
    from routers.registration import sync_invoice_registration_with_code
    try:
        logger.info(f"getting registration_response data {registration_response}")
        response_csv_data = []
        response_data = {
            "channel": "",
            "hubId": "",
            "idpId": "",
            "requestID": "",
            "groupingId": "",
            # "APIName": "Invoice Reg With EC",
            "ledgerNo": "",
            "code": "500",
            "message": "Incorrect data"
        }
        if registration_response and not registration_response[0].get('code', ''):
            for reg_request_packet in registration_response:
                merchant_details = db.query(MerchantDetails).filter(
                    MerchantDetails.unique_id == reg_request_packet.get('idpId')).first()
                response_data = {
                    "channel": reg_request_packet.get('channel'),
                    "hubId": reg_request_packet.get('hubId'),
                    "idpId": reg_request_packet.get('idpId'),
                    "requestID": reg_request_packet.get('requestId'),
                    "groupingId": reg_request_packet.get('groupingId'),
                    # "APIName": "Invoice Reg With EC",
                    "ledgerNo": "",
                    "code": "",
                    "message": ""
                }

                validate_column_response = validate_column(reg_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                logger.info(f"getting request id {reg_request_packet.get('requestId')}")
                if merchant_details:
                    reg_request_packet.pop('channel')
                    reg_request_packet.pop('hubId')
                    reg_request_packet.pop('idpId')
                    reg_request_packet.pop('total_invoice')
                    logger.info(f"getting registration request packet {reg_request_packet}")
                    reg_signature = utils.create_signature(reg_request_packet, merchant_details.merchant_secret)
                    reg_request_packet.update({"signature": reg_signature})
                    try:
                        pay_load_json = AsyncInvoiceRegistrationWithCodeSchema(**reg_request_packet)
                        pay_load = jsonable_encoder(pay_load_json)
                    except HTTPException as httpExc:
                        logger.info(f"httpExc:: {httpExc.detail}")
                        pay_load = {"code": 400, "error": str(httpExc.detail)}
                    except Exception as e:
                        logger.info(f" Exception:: {e}")
                        pay_load = {"code": 500, "error": str(e)}
                    logger.info(f"getting invoice registration schema response >>>>>>>>>>>> {pay_load}")
                    if not pay_load.get('error', ''):
                        reg_json_response = sync_invoice_registration_with_code(merchant_details.merchant_key, pay_load,
                                                                                db)
                        logger.info(f"getting registration response >>>>>>>>>>> {reg_json_response}")
                        response_data.update({
                            # "APIName": "Invoice Reg Without EC",
                            "code": reg_json_response.get('code'),
                            "message": reg_json_response.get('message'),
                            "ledgerNo": reg_json_response.get('ledgerNo', ''),
                        })
                        response_csv_data.append(response_data)
                    else:
                        logger.info(f"getting error of registration schema ########## {pay_load}")
                        response_data.update({"code": "", "message": pay_load.get('error')})
                        response_csv_data.append(response_data)
        else:
            logger.info(f"getting error of csv reading ########## {registration_response}")
            if registration_response:
                response_data = {
                    "channel": '',
                    "hubId": '',
                    "idpId": '',
                    "requestID": '',
                    "groupingId": '',
                    # "APIName": "Invoice Reg Without EC",
                    "ledgerNo": "",
                    "code": registration_response[0].get('code', ''),
                    "message": registration_response[0].get('message', '')
                }
            response_csv_data.append(response_data)

        return response_csv_data

    except Exception as e:
        logger.info("Exception occurs while download error files from xms server {}".format(e))
        return False


# REGISTRATION WITH CODE AND FINANCE RETURN API CALL RESPONSE
def do_reg_with_code_fin(registration_response):
    db = next(get_db())
    from routers.registration import sync_registration_without_code, sync_invoice_registration_with_code
    from routers.finance import sync_finance, sync_disbursement
    try:
        logger.info(f"getting registration_response data {registration_response}")
        response_data = {
            "channel": "",
            "hubId": "",
            "idpId": "",
            "requestID": "",
            "groupingId": "",
            "APIName": "Invoice Reg With EC",
            "ledgerNo": "",
            "code": "500",
            "message": "Incorrect data",
            "invoiceNo": "",
            "invoiceDate": "",
            "invoiceAmt": "",
            "invoiceStatus": "",
            "fundedAmt": "",
            "gstVerificationStatus": ""
        }
        response_csv_data = []
        if registration_response and not registration_response[0].get('code', ''):
            for reg_request_packet in registration_response:
                merchant_details = db.query(MerchantDetails).filter(
                    MerchantDetails.unique_id == reg_request_packet.get('idpId')).first()
                response_data = {
                    "channel": reg_request_packet.get('channel'),
                    "hubId": reg_request_packet.get('hubId'),
                    "idpId": reg_request_packet.get('idpId'),
                    "requestID": reg_request_packet.get('requestId'),
                    "groupingId": reg_request_packet.get('groupingId'),
                    "APIName": "Invoice Reg With EC",
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

                validate_column_response = validate_column(reg_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                logger.info(f"getting request id {reg_request_packet.get('requestId')}")
                if merchant_details:
                    finance_data = reg_request_packet.get('financeData')
                    reg_request_packet.pop('channel')
                    reg_request_packet.pop('hubId')
                    reg_request_packet.pop('idpId')
                    reg_request_packet.pop('total_invoice')
                    reg_request_packet.pop('financeData')
                    logger.info(f"getting registration request packet {reg_request_packet}")
                    reg_signature = utils.create_signature(reg_request_packet, merchant_details.merchant_secret)
                    reg_request_packet.update({"signature": reg_signature})
                    try:
                        pay_load_json = AsyncInvoiceRegistrationWithCodeSchema(**reg_request_packet)
                        pay_load = jsonable_encoder(pay_load_json)
                    except HTTPException as httpExc:
                        logger.info(f"httpExc:: {httpExc.detail}")
                        pay_load = {"code": 400, "error": str(httpExc.detail)}
                    except Exception as e:
                        logger.info(f" Exception:: {e}")
                        pay_load = {"code": 500, "error": str(e)}
                    logger.info(f"getting invoice registration schema response >>>>>>>>>>>> {pay_load}")
                    if not pay_load.get('error', ''):
                        reg_json_response = sync_invoice_registration_with_code(merchant_details.merchant_key, pay_load,
                                                                                db)
                        logger.info(f"getting registration response >>>>>>>>>>> {reg_json_response}")
                        response_data.update({
                            "APIName": "Invoice Reg With EC",
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
                                    finance_pay_load_json = AsyncBulkFinanceSchema(**finance_request_packet)
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
                                                "gstVerificationStatus": finance_response.get(
                                                    'gstVerificationStatus')
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
        else:
            logger.info(f"getting error of csv reading ########## {registration_response}")
            if registration_response:
                response_data = {
                    "channel": '',
                    "hubId": '',
                    "idpId": '',
                    "requestID": '',
                    "groupingId": '',
                    "APIName": "Invoice Reg With EC",
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

        return response_csv_data

    except Exception as e:
        logger.info("Exception occurs while download error files from xms server {}".format(e))
        return False


# ENTITY REGISTRATION RETURN API CALL RESPONSE
def do_entity_reg(registration_response):
    db = next(get_db())
    from routers.registration import sync_entity_registration
    try:
        logger.info(f"getting registration_response data {registration_response}")
        response_data = {
            "channel": "",
            "hubId": "",
            "idpId": "",
            "requestID": "",
            "code": "500",
            "message": "Incorrect data"
        }

        response_csv_data = []
        if registration_response and not registration_response[0].get('code', ''):
            # if not registration_response[0].get('code', ''):
            for reg_request_packet in registration_response:
                merchant_details = db.query(MerchantDetails).filter(
                    MerchantDetails.unique_id == reg_request_packet.get('idpId')).first()
                response_data = {
                    "channel": reg_request_packet.get('channel'),
                    "hubId": reg_request_packet.get('hubId'),
                    "idpId": reg_request_packet.get('idpId'),
                    "requestID": reg_request_packet.get('requestId'),
                    "code": "",
                    "message": ""
                }
                logger.info(f" ********* getting single request packet {reg_request_packet} ******** ")
                if int(reg_request_packet.get('totalEntityRequest')) != reg_request_packet.get('totalEntityCount'):
                    validate_response = {**ErrorCodes.get_error_response(1096)}
                    response_data.update(validate_response)
                    response_csv_data.append(response_data)
                    continue

                total_identifier_count = reg_request_packet.get('totalIdentifiersCount')
                # total_identifier_count = reg_request_packet.get('totalIdentifiersCount')
                # entity_identifier_count = 0
                logger.info(f"getting register data {reg_request_packet.get('entityRegisterData')}")
                # for entity_request_packet in reg_request_packet.get('entityRegisterData'):
                #     entity_identifier_count = entity_identifier_count + len(
                #         entity_request_packet.get('entityIdentifierData'))
                #
                # if total_identifier_count != entity_identifier_count:
                #     validate_response = {**ErrorCodes.get_error_response(1097)}
                #     response_data.update(validate_response)
                #     response_csv_data.append(response_data)
                #     continue

                total_identifier_request = 0
                for entity_request_packet in reg_request_packet.get('entityRegisterData'):
                    total_identifier_request = total_identifier_request + int(
                        entity_request_packet.get('totalIdentifiersRequest', '1'))
                    entity_request_packet.pop('totalIdentifiersRequest', '')

                if total_identifier_count != total_identifier_request:
                    validate_response = {**ErrorCodes.get_error_response(1097)}
                    response_data.update(validate_response)
                    response_csv_data.append(response_data)
                    continue

                validate_column_response = validate_column(reg_request_packet, merchant_details, db)
                if validate_column_response.get('code') != 200:
                    response_data.update(validate_column_response)
                    response_csv_data.append(response_data)
                    continue

                logger.info(f"getting request id {reg_request_packet.get('requestId')}")
                if merchant_details:
                    reg_request_packet.pop('channel')
                    reg_request_packet.pop('hubId')
                    reg_request_packet.pop('idpId')
                    reg_request_packet.pop('totalEntityRequest')
                    reg_request_packet.pop('totalIdentifiersRequest')
                    reg_request_packet.pop('totalEntityCount')
                    reg_request_packet.pop('totalIdentifiersCount')
                    logger.info(f"getting registration request packet {reg_request_packet}")
                    reg_signature = utils.create_signature(reg_request_packet, merchant_details.merchant_secret)
                    reg_request_packet.update({"signature": reg_signature})
                    try:
                        pay_load_json = EntityRegistrationSchema(**reg_request_packet)
                        pay_load = jsonable_encoder(pay_load_json)
                    except HTTPException as httpExc:
                        logger.info(f"httpExc:: {httpExc.detail}")
                        pay_load = {"code": 400, "error": str(httpExc.detail)}
                    except Exception as e:
                        logger.info(f" Exception:: {e}")
                        pay_load = {"code": 500, "error": str(e)}
                    logger.info(f"getting invoice registration schema response >>>>>>>>>>>> {pay_load}")
                    if not pay_load.get('error', ''):
                        reg_json_response = sync_entity_registration(merchant_details.merchant_key, pay_load,
                                                                     db)
                        logger.info(f"getting registration response >>>>>>>>>>> {reg_json_response}")
                        response_data.update({
                            "code": reg_json_response.get('code'),
                            "message": reg_json_response.get('message')
                        })
                        response_csv_data.append(response_data)
                    else:
                        logger.info(f"getting error of registration schema ########## {pay_load}")
                        response_data.update({"code": "", "message": pay_load.get('error')})
                        response_csv_data.append(response_data)
        else:
            logger.info(f"getting error of csv reading ########## {registration_response}")
            if registration_response:
                response_data = {
                    "channel": '',
                    "hubId": '',
                    "idpId": '',
                    "requestID": '',
                    "code": registration_response[0].get('code', ''),
                    "message": registration_response[0].get('message', '')
                }
            response_csv_data.append(response_data)

        return response_csv_data

    except Exception as e:
        logger.info("Exception occurs while download error files from xms server {}".format(e))
        return False


def fetch_upload_file_to_sftp():
    import pysftp
    try:
        logger.info(f"fetch_upload_file_to_sftp................ ")
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        remote_dir = '/'
        with pysftp.Connection(
                host=__SFTP_HOST,
                username=__SFTP_USERNAME,
                password=__SFTP_PASSWORD,
                cnopts=cnopts,
                port=__SFTP_PORT
        ) as sftp:
            logger.info(f"fetch_upload_file_to_sftp :: sftp connection establish !!!")

            with sftp.cd(remote_dir):
                for merchant_dir in sftp.listdir(remote_dir):
                    logger.info(f" merchant_dir :: {merchant_dir}")
                    from config import async_sftp_idp_process
                    # async_sftp_idp_process.delay(merchant_dir)
                    async_sftp_idp_process(merchant_dir)
                    # await async_sftp_idp_process(merchant_dir)
            # sftp.close()
    except Exception as e:
        logger.exception(f"Exception fetch_upload_file_to_sftp :: {e}")


def move_file_to_sftp_dir(sftp, rmt_src, rmt_dest):
    if sftp.exists(rmt_dest):
        permissions = sftp.stat(rmt_src).st_mode
        source_owner = sftp.stat(rmt_src).st_uid
        source_group = sftp.stat(rmt_src).st_gid
        # Remove the file
        sftp.remove(rmt_dest)
        sftp.rename(rmt_src, rmt_dest)
        # sftp.chmod(rmt_dest, permissions)
        # sftp.chown(rmt_dest, source_owner, source_group)
    else:
        # permissions = sftp.stat(rmt_src).st_mode
        sftp.rename(rmt_src, rmt_dest)
        # sftp.chmod(rmt_dest, permissions)


def send_reject_email(user_email, file_name, customer, user_name, success_rec, failed_rec, messages, total_rec,
                      body_text):
    from config import async_sftp_send_email
    if __SEND_EMAIL:
        subject = f"{__ENV_NAME.lower()}:{file_name} is rejected for Customer: {customer} by User: {user_name}"

        email_to = user_email or dconfig('SUPPORT_EMAIL', "arpit.bansal@citycash.in")
        body_data = {
            "user": "",
            "fileName": file_name,
            "customer": customer,
            "username": user_name,
            "insertFileName": file_name,
            "recordSuccessCount": success_rec,
            "recordFailedCount": failed_rec,
            "listReasonsApplicable": messages or "",
            "totalCount": total_rec
        }
        async_sftp_send_email.delay(subject, email_to, body_data, body_text=body_text)


def process_sftp_idp(merchant_dir):
    logger.info("inside :: process_sftp_idp ::")
    from routers.sftp_headers_config import SFTP_API_HEADERS
    from routers.send_mail import REJECT_BODY_TEXT, INVALID_BODY_TEXT, SUCCESS_BODY_TEXT
    import pysftp
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    with pysftp.Connection(
            host=__SFTP_HOST,
            username=__SFTP_USERNAME,
            password=__SFTP_PASSWORD,
            cnopts=cnopts,
            port=__SFTP_PORT
    ) as sftp:

        idp_file_path = f'/{merchant_dir}/input/'
        # change dir
        with sftp.cd(idp_file_path):
            logger.info(f"process_sftp_idp :: goes inside:  cd {idp_file_path}")
            remote_src = f'/{merchant_dir}/input/'
            remote_process_dest = f'/{merchant_dir}/inProcess/'
            remote_processed_dest = f'/{merchant_dir}/processed/'
            remote_output_dest = f'/{merchant_dir}/output/'
            # remote_delivered_dest = f'/{merchant_dir}/delivered/'
            remote_error_input_dest = f'/{merchant_dir}/errorInput/'
            remote_error_output_dest = f'/{merchant_dir}/errorOutput/'
            logger.info(f"process_sftp_idp :: list down files :  {sftp.listdir()}")

            for file_name in sftp.listdir():
                start_time = time.time()
                new_file_path = remote_src + file_name
                # permissions = sftp.stat(new_file_path).st_mode
                # # Get the owner (user and group) of the source file
                # source_owner = sftp.stat(new_file_path).st_uid
                # source_group = sftp.stat(new_file_path).st_gid
                if not file_name.endswith('.csv'):
                    rmt_src = remote_src + file_name
                    rmt_dest = remote_error_input_dest + file_name
                    logger.info(f"move file to errorInput folder: src : {rmt_src},  dest : {rmt_dest}")
                    move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)
                    #trigger email for error input
                    send_reject_email('', file_name, "Test", "User", 0, 0, "", 0,
                                      body_text=INVALID_BODY_TEXT)
                    continue
                logger.info(f"yes, we've found .csv file :: in  :: {idp_file_path}{file_name}")
                try:
                    sep_sign = '%'
                    if __ENV_NAME.lower() in ('dev', 'uat', 'pre_uat'):
                        if file_name.lower().startswith(f"{__ENV_NAME.lower()}{sep_sign}{sep_sign}"):
                            splited_file_name = file_name.lower().split(f'{sep_sign}{sep_sign}')[1].split(sep_sign)
                            # n_file_name = file_name.lower().split(f'{sep_sign}{sep_sign}')[1].split(sep_sign)[1]
                            n_file_name = splited_file_name[1]
                            user_id = splited_file_name[2]
                            user_name = splited_file_name[3]
                            total_rec = splited_file_name[7].split(".")[0]
                            customer = "test"
                        else:
                            rmt_src = remote_src + file_name
                            rmt_dest = remote_error_input_dest + file_name
                            logger.info(f"move file to errorInput folder: src : {rmt_src},  dest : {rmt_dest}")
                            move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)

                            # trigger email for error input
                            send_reject_email('', file_name, "Test", "User", 0, 0, "", 0,
                                              body_text=INVALID_BODY_TEXT)
                            continue
                    elif __ENV_NAME.lower() == 'prod':
                        n_file_name = file_name.lower().split(sep_sign)[1]
                        user_id = n_file_name[2]
                        user_name = n_file_name[3]
                        total_rec = n_file_name[7].split(".")[0]
                        customer = "test"
                    else:
                        logger.info(f"{__ENV_NAME.lower()} does not configured!")
                        rmt_src = remote_src + file_name
                        rmt_dest = remote_error_input_dest + file_name
                        logger.info(f"move file to errorInput folder: src : {rmt_src},  dest : {rmt_dest}")
                        move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)
                        # trigger email for error input
                        send_reject_email('', file_name, "Test", "User", 0, 0, "", 0,
                                          body_text=INVALID_BODY_TEXT)
                        continue
                except Exception as e:
                    logger.exception(f"Exception occurs while matching environment {e}")
                    rmt_src = remote_src + file_name
                    rmt_dest = remote_error_input_dest + file_name
                    logger.info(f"move file to errorInput folder: src : {rmt_src},  dest : {rmt_dest}")
                    move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)
                    # trigger email for error input
                    send_reject_email('', file_name, "Test", "User", 0, 0, "", 0,
                                      body_text=INVALID_BODY_TEXT)
                    continue

                n_file_name = n_file_name + '.csv'
                if n_file_name.lower() in ['invoice_reg_without_entity_code_finance_disbursement.csv',
                                           'disbursement.csv',
                                           'repayment.csv', 'invoice_reg_without_entity_code_finance.csv',
                                           'finance.csv', 'disbursement_repayment.csv',
                                           'invoice_reg_without_entity_code.csv',
                                           'invoice_reg_with_entity_code.csv',
                                           'invoice_reg_with_entity_code_finance.csv',
                                           'invoice_reg_with_entity_code_finance_disbursement.csv', 'cancel.csv',
                                           'entity_registration.csv', 'validate_with_entity_code.csv',
                                           'validate_without_entity_code.csv']:
                    try:
                        logger.info(f"let's, process file :: {file_name}")
                        api_response_csv = []
                        valid_names = ["hub", "direct"]
                        with sftp.open(file_name) as f:
                            is_col_data_invalid = False
                            # move file to process folder
                            rmt_src = remote_src + file_name
                            rmt_dest = remote_process_dest + file_name
                            logger.info(f"move file to process folder: src : {rmt_src},  dest : {rmt_dest}")
                            move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)
                            chunk_size = 10000  # Number of rows per chunk

                            # Initialize an empty list to store chunks
                            chunks = []
                            # Iterate over the CSV file in chunks
                            try:
                                for chunk in pd.read_csv(f, delimiter="|", dtype=str, na_values='', index_col=False,
                                                         chunksize=chunk_size):
                                    # Process each chunk as needed (e.g., perform computations, filter rows, etc.)
                                    # For demonstration purposes, we'll just print the chunk shape
                                    logger.info(f"Chunk shape is: {chunk.shape}")

                                    # Append the chunk to the list of chunks
                                    chunks.append(chunk)
                                df = pd.concat(chunks, ignore_index=True)
                                df.drop(df.columns[df.columns.str.contains('unnamed', case=False)], axis=1,
                                        inplace=True)
                                # Find the index of the last non-empty row
                                last_non_empty_row_index = df.last_valid_index()
                                # Slice the DataFrame up to the last non-empty row
                                df = df.loc[:last_non_empty_row_index]
                                df = df.replace(np.nan, '')

                                col_list = df.columns.tolist()
                                logger.info(f"col_list >>>>>>>>>>>> {col_list}")
                                if 'channel' in col_list:
                                    sftp_header_list = list(SFTP_API_HEADERS().api_with_cols.get(n_file_name))
                                    logger.info(f"{sftp_header_list}")

                                    if len(sftp_header_list) != len(col_list) and set(sftp_header_list) != set(
                                            col_list):
                                        logger.info(f"inside header wrong {sftp_header_list != col_list}")
                                        is_col_data_invalid = True
                                        header_error = ErrorCodes.get_error_response(1090)
                                        header_error.update({
                                            "message": f"{header_error.get('message')}:: it should be like \n {','.join(sftp_header_list)}"})
                                        api_response_csv = [header_error]
                                    elif df['channel'].str.strip().str.lower().nunique() != 1:
                                        is_col_data_invalid = True
                                        api_response_csv = [ErrorCodes.get_error_response(1095)]
                                    else:
                                        if all(df['channel'].str.strip().str.lower().isin(valid_names)):
                                            is_idp_unique = (df['channel'].str.strip().str.lower().isin(
                                                ['direct', 'hub']) & (df['idpId'].str.strip().nunique() == 1))
                                            if not is_idp_unique[0]:
                                                is_col_data_invalid = True
                                                api_response_csv = [ErrorCodes.get_error_response(1093)]
                                        elif all(df['channel'].str.strip().str.lower().isin(['ibdic'])):
                                            pass
                                        else:
                                            is_col_data_invalid = True
                                            api_response_csv = [ErrorCodes.get_error_response(1095)]

                            except Exception as e:
                                logger.exception(f"Exception while reading file  >>>>>>>>> {e}")
                                csv_error = ErrorCodes.get_error_response(1131)
                                error_message = csv_error.get("message")
                                csv_error.get("message").update({"message": f"{error_message} :: {str(e)}"})
                                api_response_csv = [csv_error]

                        # asia_kolkata = pytz.timezone('Asia/Kolkata')
                        # current_date_time = dt.datetime.now(asia_kolkata).strftime("%d%m%Y_%H_%M_%S")
                        # resp_file_name = file_name.split('.')[0] + '_' + current_date_time + '_response.csv'
                        # resp_file_name = file_name.split('.')[0] + '_response.csv'

                        ### dynamically get function, then call respective api
                        if not api_response_csv:
                            api_response_csv = SftpFileMapper.file_func_mapper.get(n_file_name)(df)
                            logger.info(f"getting api_response_csv :: {api_response_csv}")
                            if not isinstance(api_response_csv, list):
                                api_response_csv = [api_response_csv]
                            if api_response_csv and api_response_csv[0] and api_response_csv[0].get('code', '') == 1090:
                                is_col_data_invalid = True
                                api_response_csv = [ErrorCodes.get_error_response(1090)]

                        df = pd.DataFrame(api_response_csv)
                        resp_col_list = df.columns.tolist()

                        buffer_data = io.StringIO()
                        df.to_csv(buffer_data, sep="|", index=False)
                        buffer_data.seek(0)
                        logger.info(f"resp_col_list::::::: {resp_col_list}")
                        # logger.info("")
                        db = next(get_db())
                        user_email = False
                        if 'requestID' in resp_col_list:
                            success_errors_code = [200, 1013, 1004, 1027, 1031]
                            total_rec = df['requestID'].nunique()
                            failed_rec = len(df[~df['code'].isin(success_errors_code)])
                            success_rec = total_rec - failed_rec
                            resp_file_name = f"{file_name.split('.')[0]}{sep_sign}{success_rec}{sep_sign}{failed_rec}.csv"
                            messages = ','.join(list(filter(lambda x: x is not None, list(
                                map(lambda msg: msg['message'] if msg['code'] not in success_errors_code else None,
                                    api_response_csv)))))
                            logger.info(f"messages : {messages}")
                            user_email = db.query(
                                SFTPUserInfo
                            ).filter(
                                SFTPUserInfo.extra_data.contains(
                                    {"userType": "SFTP", "SFTPUsername": user_name.strip()})
                            ).first()
                        else:
                            total_rec = 0
                            failed_rec = 0
                            success_rec = total_rec - failed_rec
                            resp_file_name = f"{file_name.split('.')[0]}{sep_sign}0{sep_sign}0.csv"
                            messages = ", ".join([msgs['message'] for msgs in api_response_csv]) if isinstance(
                                api_response_csv, list) else ""
                            logger.info(f"messages : {messages}")
                            user_email = db.query(
                                SFTPUserInfo
                            ).filter(
                                SFTPUserInfo.extra_data.contains(
                                    {"userType": "SFTP", "SFTPUsername": user_name.strip()})
                            ).first()
                        logger.info(f"USer email :::::: {user_email}:: {user_email and user_email.email_address}")
                        if is_col_data_invalid:
                            rmt_src = remote_process_dest + file_name
                            rmt_dest = remote_error_input_dest + file_name
                            logger.info(f"move file to errorInput folder: src : {rmt_src},  dest : {rmt_dest}")
                            move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)

                            # permissions = sftp.stat(rmt_src).st_mode
                            # write resp file to errorOutput folder
                            sftp.putfo(buffer_data, remote_error_output_dest + resp_file_name)
                            sftp.chmod(remote_error_output_dest + resp_file_name, mode=664)
                            # sftp.chown(remote_error_output_dest + resp_file_name, source_owner, source_group)
                            # trigger email if file rejected
                            email_to = user_email and user_email.email_address or ''

                            send_reject_email(email_to, file_name, customer, user_name, success_rec, failed_rec,
                                              messages, total_rec,
                                              body_text=REJECT_BODY_TEXT)
                        else:
                            # permissions = sftp.stat(remote_process_dest + file_name).st_mode
                            # write resp file to output folder
                            sftp.putfo(buffer_data, remote_output_dest + resp_file_name)
                            logger.info("assign permission as it is like original file")
                            sftp.chmod(remote_output_dest + resp_file_name, mode=664)
                            # sftp.chown(remote_output_dest + resp_file_name, source_owner, source_group)

                            # move file to delivered folder
                            rmt_src = remote_process_dest + file_name
                            rmt_dest = remote_processed_dest + file_name
                            logger.info(
                                f"move file from process to processed folder: src : {rmt_src},  dest : {rmt_dest}")

                            # move response file to output folder.
                            move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)

                            # trigger email if file rejected
                            email_to = user_email and user_email.email_address or ''
                            send_reject_email(email_to, file_name, customer, user_name, success_rec, failed_rec,
                                              messages,
                                              total_rec,
                                              body_text=SUCCESS_BODY_TEXT)

                    except Exception as e:
                        logger.exception(f"Exception putting locally generate response on sftp server :: {e}")
                        rmt_src = remote_processed_dest + file_name
                        rmt_dest = remote_error_input_dest + file_name
                        logger.info(f"move file to errorInput folder: src : {rmt_src},  dest : {rmt_dest}")
                        move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)

                        # trigger email for error input
                        send_reject_email('', file_name, "Test", "User", 0, 0, "", 0,
                                          body_text=INVALID_BODY_TEXT)
                    finally:
                        db.close()
                else:
                    rmt_src = remote_src + file_name
                    rmt_dest = remote_error_input_dest + file_name
                    logger.info(f"move file to errorInput folder: src : {rmt_src},  dest : {rmt_dest}")
                    move_file_to_sftp_dir(sftp, rmt_src, rmt_dest)

                    # trigger email for error input
                    send_reject_email('', file_name, "Test", "User", 0, 0, "", 0,
                                      body_text=INVALID_BODY_TEXT)
                logger.info(f"Time taken to process file {n_file_name} is {time.time() - start_time}")


async def send_email_to_idp_for_processed_file(file_name, user_name, customer, success_rec, failed_rec, total_rec,
                                               list_reason_applicable=""):
    logger.info(f"inside...send_email_to_idp_for_processed_file...")
    try:
        subject = f"{file_name} is processed for Customer: {customer} by User: {user_name}"
        email_to = "kailash.bani@citycash.in"
        body_data = {
            "user": user_name,
            "fileName": file_name,
            "customer": customer,
            "username": user_name,
            "insertFileName": file_name,
            "recordSuccessCount": success_rec,
            "recordFailedCount": failed_rec,
            "listReasonsApplicable": list_reason_applicable or "",
            "totalCount": total_rec
        }
        await send_email_async(subject, email_to, body_data)
    except Exception as e:
        logger.info(f"Exception :: send_email_to_idp_for_processed_file : {e}")
        pass


def prepare_request_data_of_idp_without_ec_reg_fin_dis(df):
    request_packets = RegistrationWithOutECFinanceDisbursementVaiCSV.read_csv_to_reg_without_code_fin_dis(df)
    api_resp = RegistrationWithOutECFinanceDisbursementVaiCSV.do_reg_without_code_fin_disb(request_packets)
    return api_resp


def prepare_request_data_of_idp_financing(df):
    finance_request_packets = FinancingVaiCSV.read_csv_to_financing(df)
    api_resp = FinancingVaiCSV.do_financing(finance_request_packets)
    return api_resp


def prepare_request_data_of_idp_disbursement_repayment(df):
    disbursement_response = DisbursementRepaymentVaiCSV.read_csv_to_disbursement_repayment(df)
    api_resp = DisbursementRepaymentVaiCSV.do_disbursement_and_repayment(disbursement_response)
    return api_resp


def prepare_request_data_of_idp_with_ec_reg_fin_dis(df):
    request_packets = RegistrationWithECFinanceDisbursementVaiCSV.read_csv_to_reg_with_ec_fin_dis(df)
    api_resp = RegistrationWithECFinanceDisbursementVaiCSV.do_reg_with_ec_fin_disb(request_packets)
    return api_resp


def prepare_request_data_of_idp_val_ser_with_code(df):
    val_ser_request_packets = ValidateWithEntityCodeVaiCSV.read_csv_to_validation_service_with_code(df)
    api_resp = ValidateWithEntityCodeVaiCSV.do_validation_service_with_code(val_ser_request_packets)
    return api_resp


def prepare_request_data_of_idp_val_ser_without_code(df):
    val_ser_request_packets = ValidateWithoutEntityCodeVaiCSV.read_csv_to_validation_service_without_code(df)
    api_resp = ValidateWithoutEntityCodeVaiCSV.do_validation_service_without_code(val_ser_request_packets)
    return api_resp


def prepare_request_data_of_idp_reg_without_code_fin(df):
    registration_response = read_csv_reg_without_code_finance(df)
    csv_resp = do_reg_without_code_fin(registration_response)
    return csv_resp


def prepare_request_data_of_idp_disbursement(df):
    disbursement_response = dis_read_csv_file(df)
    csv_resp = do_disbursement(disbursement_response)
    return csv_resp


def prepare_request_data_of_idp_reg_without_code(df):
    registration_response = read_csv_reg_without_code(df)
    csv_resp = do_reg_without_code(registration_response)
    return csv_resp


def prepare_request_data_of_idp_repayment(df):
    repayment_response = repay_read_csv_file(df)
    csv_resp = do_repayment(repayment_response)
    return csv_resp


def prepare_request_data_of_idp_cancellation(df):
    cancellation_response = cancel_read_csv_file(df)
    csv_resp = do_cancellation(cancellation_response)
    return csv_resp


# PREPARE REQUEST FOR REGISTRATION WITH CODE
def prepare_request_data_of_idp_reg_with_code(df):
    registration_response = read_csv_reg_with_code(df)
    csv_resp = do_reg_with_code(registration_response)
    return csv_resp


# PREPARE REQUEST FOR REGISTRATION WITH CODE AND FINANCE
def prepare_request_data_of_idp_reg_with_code_finance(df):
    registration_response = read_csv_reg_with_code_finance(df)
    csv_resp = do_reg_with_code_fin(registration_response)
    return csv_resp


# PREPARE REQUEST FOR ENTITY REGISTRATION
def prepare_request_data_of_idp_entity_reg(df):
    registration_response = read_csv_entity_reg(df)
    csv_resp = do_entity_reg(registration_response)
    return csv_resp


class SftpFileMapper:
    entity_registration = "entity_registration.csv"
    invoice_reg_with_entity_code = 'invoice_reg_with_entity_code.csv'
    invoice_reg_without_entity_code = 'invoice_reg_without_entity_code.csv'
    finance = 'finance.csv'
    cancel = 'cancel.csv'
    disbursement = 'disbursement.csv'
    repayment = 'repayment.csv'
    validate_with_entity_code = 'validate_with_entity_code.csv'
    validate_without_entity_code = 'validate_without_entity_code.csv'
    invoice_reg_with_entity_code_finance = 'invoice_reg_with_entity_code_finance.csv'
    invoice_reg_with_entity_code_finance_disbursement = 'invoice_reg_with_entity_code_finance_disbursement.csv'
    invoice_reg_without_entity_code_finance = 'invoice_reg_without_entity_code_finance.csv'
    invoice_reg_without_entity_code_finance_disbursement = 'invoice_reg_without_entity_code_finance_disbursement.csv'
    disbursement_repayment = 'disbursement_repayment.csv'

    file_func_mapper = {
        invoice_reg_without_entity_code_finance_disbursement: prepare_request_data_of_idp_without_ec_reg_fin_dis,
        invoice_reg_with_entity_code_finance_disbursement: prepare_request_data_of_idp_with_ec_reg_fin_dis,
        invoice_reg_without_entity_code_finance: prepare_request_data_of_idp_reg_without_code_fin,
        disbursement: prepare_request_data_of_idp_disbursement,
        invoice_reg_without_entity_code: prepare_request_data_of_idp_reg_without_code,
        repayment: prepare_request_data_of_idp_repayment,
        finance: prepare_request_data_of_idp_financing,
        disbursement_repayment: prepare_request_data_of_idp_disbursement_repayment,
        entity_registration: prepare_request_data_of_idp_entity_reg,
        invoice_reg_with_entity_code: prepare_request_data_of_idp_reg_with_code,
        invoice_reg_with_entity_code_finance: prepare_request_data_of_idp_reg_with_code_finance,
        cancel: prepare_request_data_of_idp_cancellation,
        validate_with_entity_code: prepare_request_data_of_idp_val_ser_with_code,
        validate_without_entity_code: prepare_request_data_of_idp_val_ser_without_code
    }


def validate_column(reg_request_packet, merchant_details, db):
    # logger.info(f"getting request id before channel {reg_request_packet.get('requestId')}")
    validate_response = {**ErrorCodes.get_error_response(200)}
    # if reg_request_packet.get('channel', '').strip() == "":
    #     validate_response = {**ErrorCodes.get_error_response(1082)}
    #
    # logger.info(f"getting request id before len channel {reg_request_packet.get('requestId')}")
    # if len(reg_request_packet.get('channel')).strip() > 16:
    #     validate_response = {**ErrorCodes.get_error_response(1086)}

    logger.info(f"getting request id before hubid {reg_request_packet.get('requestId')}")
    if reg_request_packet.get('hubId').strip() == "":
        validate_response = {**ErrorCodes.get_error_response(1081)}

    elif not reg_request_packet.get('hubId').strip().isnumeric():
        validate_response = {**ErrorCodes.get_error_response(1085)}

    logger.info(f"getting request id before idpid {reg_request_packet.get('requestId')}")
    if reg_request_packet.get('idpId').strip() == "":
        validate_response = {**ErrorCodes.get_error_response(1083)}

    logger.info(f"getting request id before ledgerData {reg_request_packet.get('requestId')}")
    if reg_request_packet.get('ledgerData', ''):
        if str(len(reg_request_packet.get('ledgerData', ''))) != str(reg_request_packet.get('total_invoice')):
            validate_response = {**ErrorCodes.get_error_response(1087)}

    logger.info(f"getting request id before total_buyer_identifier_request {reg_request_packet.get('requestId')}")
    if str(reg_request_packet.get('total_buyer_identifier_request', '')) != str(
            reg_request_packet.get('total_buyer_identifier_count', '')):
        validate_response = {**ErrorCodes.get_error_response(1088)}

    logger.info(
        f"getting request id before total_seller_identifier_request {reg_request_packet.get('requestId')}")
    if str(reg_request_packet.get('total_seller_identifier_request', '')) != str(
            reg_request_packet.get('total_seller_identifier_count', '')):
        validate_response = {**ErrorCodes.get_error_response(1089)}

    if not merchant_details:
        validate_response = {**ErrorCodes.get_error_response(1002)}
    else:
        hub_details = db.query(Hub).filter(
            Hub.id == merchant_details.hub_id).first()
        if not hub_details or hub_details.unique_id != reg_request_packet.get('hubId').strip():
            validate_response = {**ErrorCodes.get_error_response(1084)}

    return validate_response
