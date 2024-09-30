from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import JSONResponse
from fastapi.requests import Request

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@router.post("/{url_end_point}/{merchant_key}/{hub_key}",
             response_description=None,
             dependencies=[Depends(RateLimiter(times=__RATE_LIMITER_TIMES, seconds=__RATE_LIMITER_SECONDS))])
async def hub_route(url_end_point: str, merchant_key: str, hub_key: str, request: Request, token: str = Depends(oauth2_scheme)):
    # Validate the token and authenticate the user
    user = authenticate_user(token)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Unauthorized"})

    # Validate the input data
    try:
        pay_load = json.loads(request.body)
        validate_pay_load(pay_load)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": "Invalid request"})

    # Use secure cryptography practices
    key = generate_aes_key(merchant_key)
    try:
        decrypted_data = decrypt_aes_256(key, pay_load.get("encryptData"))
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": "Invalid encryptData"})

    # Implement secure error handling
    try:
        api_response = APIEndPoint.end_point_func_mapper.get(url_end_point)[0](merchant_key, decrypted_data, db)
    except Exception as e:
        logger.error(f"Error in sync_finance {e}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})

    # Implement secure logging
    logger.info(f"API response: {api_response}")

    return JSONResponse(status_code=200, content={"encryptData": encrypt_aes_256(key, api_response)})
