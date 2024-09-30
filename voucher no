
class TagsMetadata:
    voucher_purchase = {
        "name": "Voucher Purchase",
        "description": "This API allows a user to purchase vouchers. Vouchers can be used later to pay against invoices.",
        "summary": "Purchase Vouchers"
    }
    voucher_usage = {
        "name": "Voucher Usage",
        "description": "This API allows a user to use their purchased vouchers to pay against invoices.",
        "summary": "Use Vouchers"
    }
    voucher_notification = {
        "name": "Voucher Notification",
        "description": "This API notifies a user when their voucher balance falls below a specified threshold.",
        "summary": "Voucher Balance Notification"
    }

@router.post("/purchase-vouchers/{merchant_key}", description=TagsMetadata.voucher_purchase['description'])
def purchase_vouchers(merchant_key: str, request: VoucherPurchaseSchema, db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'purchase-vouchers',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    
    validate_signature_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        vouchers = utils.create_vouchers(db, jsonable_encoder(request), merchant_key)
        return_response = {"vouchers": vouchers, "code": 200, "message": "Vouchers purchased successfully."}
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

@router.post("/use-vouchers/{merchant_key}", description=TagsMetadata.voucher_usage['description'])
def use_vouchers(merchant_key: str, request: VoucherUsageSchema, db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'use-vouchers',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    
    validate_signature_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        usage_response = utils.use_vouchers(db, jsonable_encoder(request), merchant_key)
        if usage_response.get('code') == 200:
            return_response = {"code": 200, "message": "Vouchers used successfully.", **usage_response}
        else:
            return_response = usage_response
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

@router.post("/voucher-notification/{merchant_key}", description=TagsMetadata.voucher_notification['description'])
def voucher_notification(merchant_key: str, request: VoucherNotificationSchema, db: Session = Depends(get_db)):
    api_request_log_res = utils.create_request_log(
        db,
        request.requestId,
        jsonable_encoder(request),
        '',
        'request',
        'voucher-notification',
        merchant_key
    )
    if api_request_log_res.get("code") != 200:
        return {
            "requestId": request.requestId,
            **api_request_log_res
        }
    
    validate_signature_response = utils.validate_signature(db, jsonable_encoder(request), merchant_key)
    if validate_signature_response.get('code') == 200:
        notification_response = utils.check_voucher_balance(db, jsonable_encoder(request), merchant_key)
        if notification_response.get('balance') < request.threshold:
            utils.notify_user(notification_response)
            return_response = {"code": 200, "message": "Notification sent successfully.", **notification_response}
        else:
            return_response = {"code": 200, "message": "Balance is sufficient.", **notification_response}
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
