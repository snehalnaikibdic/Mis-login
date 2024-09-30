import asyncio
import time
import logging
import schemas
import models
import utils
from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from database import get_db
from routers.auth import get_password_hash
from utils import generate_key_secret
from fastapi.encoders import jsonable_encoder
from errors import ErrorCodes
# logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
router = APIRouter()


class TagsMetadata:
    create_merchant_info = {
        "name": "Registration new merchant by using API.",
        "description": "**Aim** : Registration new merchant by using this API.\n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- merchant name should be unqiue\n"
                       "- webhook url, IpsToWhitelist validation.\n"
                       "- AuthSignatory 1 - Name, department, degignation, mobile no. emailid is required with proper validation\n"
                       "- hubId is required with validation \n"
                       "- authsignatory 2 and admin 2 detail is optional.\n"
                       "\n **Table** : MerchantDetails, Hub, \n"
                       "\n **Function** : create_m, validate_pan_gst_relationship \n",
        "summary": "Summary of creart merchant"
    }
    create_hub_info = {
        "name": "create a new hub by using API.",
        "description": "**Aim** : Create a new hub by using this API.\n"
                       "\n**Validation** : \n"
                       "- Name validation\n"
                       "\n **Table** : Hub \n"
                       "\n **Function** : create_hub \n",
        "summary": "Summary of creart hub"
    }


def create_user(db: Session, user: schemas.UserCreate):
    fake_hashed_password = user.password + "notreallyhashed"
    db_user = models.User(email=user.email, password=fake_hashed_password)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


def create_m(db: Session, merchant: schemas.MerchantDetailSchema, hash_pwd: str):

    auth_signature_dict = {
        "AuthSignatoryName1": merchant.AuthSignatoryName1,
        "AuthSignatoryDepartment1": merchant.AuthSignatoryDepartment1,
        "AuthSignatoryDesignation1": merchant.AuthSignatoryDesignation1,
        "AuthSignatoryMobileNo1": merchant.AuthSignatoryMobileNo1,
        "AuthSignatoryEmailId1": merchant.AuthSignatoryEmailId1,
        "AdminName1": merchant.AdminName1,
        "AdminMobileNo1": merchant.AdminMobileNo1,
        "AdminEmailId1": merchant.AdminEmailId1,

        "AuthSignatoryName2": merchant.AuthSignatoryName2 if merchant.AuthSignatoryName2 else '',
        "AuthSignatoryDepartment2": merchant.AuthSignatoryDepartment2 if merchant.AuthSignatoryDepartment2 else '',
        "AuthSignatoryDesignation2": merchant.AuthSignatoryDesignation2 if merchant.AuthSignatoryDesignation2 else '',
        "AuthSignatoryMobileNo2": merchant.AuthSignatoryMobileNo2 if merchant.AuthSignatoryMobileNo2 else '',
        "AuthSignatoryEmailId2": merchant.AuthSignatoryEmailId2 if merchant.AuthSignatoryEmailId2 else '',
        "AdminName2": merchant.AdminName2 if merchant.AdminMobileNo2 else '',
        "AdminMobileNo2": merchant.AdminMobileNo2 if merchant.AdminMobileNo2 else '',
        "AdminEmailId2": merchant.AdminEmailId2 if merchant.AdminEmailId2 else ''
    }
    extra_data = {
        "IdpRegisterAddress": merchant.IdpRegisterAddress,
        "IdpState": merchant.IdpState,
        "IdpPan": merchant.IdpPan,
        "IdpGst": merchant.IdpGst,
        "InvPlanOpted": merchant.InvPlanOpted,
        "AuthorisedSignatoryAdminData": auth_signature_dict
    }

    db_user = models.MerchantDetails(
        name=merchant.name.strip(),
        webhook_endpoint=merchant.webhook_url,
        merchant_key=generate_key_secret(16),
        merchant_secret=generate_key_secret(32),
        username=merchant.name.strip().lower(),
        password=hash_pwd,
        extra_data=extra_data
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


def create_h(db: Session, merchant: schemas.HubDetailSchema):
    db_user = models.Hub(
        name=merchant.name.strip(),
        hub_key=generate_key_secret(16),
        hub_secret=generate_key_secret(32),
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user
# @router.post("/users/", response_model=schemas.User)
def create_user1(user: schemas.UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(models.User).filter(models.User.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=401, detail="Email already registered")
    user_cr = create_user(db=db, user=user)
    return JSONResponse({"user_created": user_cr.id})


@router.post("/create-merchant/", response_model=schemas.MerchantDetailSchema, description=TagsMetadata.create_merchant_info.get('description'))
def create_merchant(merchant: schemas.MerchantDetailSchema, db: Session = Depends(get_db)):
    # db_user = db.query(models.MerchantDetails).filter(models.MerchantDetails.name == merchant.name).first()

    merchant_name = merchant.name.strip().lower()
    db_user = db.query(models.MerchantDetails).filter(models.MerchantDetails.username == merchant_name).first()
    # Invalid gst or pan
    merchant_pan_response = utils.validate_pan_gst_relationship(merchant.IdpPan, merchant.IdpGst)
    if not merchant_pan_response:
        raise HTTPException(status_code=400, detail="IDP Incorrect GST or PAN")

    idp_gst_response = utils.validate_idp_gst(db, jsonable_encoder(merchant))
    if idp_gst_response:
        raise HTTPException(status_code=400, detail="IDP GST already registered")

    idp_pan_response = utils.validate_idp_pan(db, jsonable_encoder(merchant))
    if idp_pan_response:
        raise HTTPException(status_code=400, detail="IDP PAN already registered")
    if db_user:
        raise HTTPException(status_code=401, detail="Merchant already registered")

    hub_obj = db.query(models.Hub).filter(models.Hub.unique_id == merchant.hubId).first()
    if not hub_obj:
        raise HTTPException(status_code=401, detail="Hub not found")

    pwd = "secret"
    has_pwd = get_password_hash(pwd)
    user_cr = create_m(db=db, merchant=merchant, hash_pwd=has_pwd)
    # print(f"merchant extra_data------,{user_cr.extra_data}")
    db.query(models.MerchantDetails).filter(models.MerchantDetails.id == user_cr.id).update({
        'unique_id': str(user_cr.id).zfill(4),
        'hub_id': hub_obj.id
    })
    db.commit()
    return JSONResponse({"merchant_created": user_cr.merchant_key,
                         "merchantId": str(user_cr.id).zfill(4),
                         "merchantData": user_cr.extra_data})

@router.post("/create-hub/", response_model=schemas.HubDetailSchema, description=TagsMetadata.create_hub_info.get('description'))
def create_hub(hub: schemas.HubDetailSchema, db: Session = Depends(get_db)):

    merchant_name = hub.name.strip().lower()
    db_user = db.query(models.Hub).filter(models.Hub.name.ilike(merchant_name)).first()
    if not db_user:
        user_cr = create_h(db=db, merchant=hub)
        logger.info(f"Created Hub {user_cr.id}")
        db.query(models.Hub).filter(models.Hub.id==user_cr.id).update({'unique_id': str(user_cr.id).zfill(4)})
        db.commit()
        return JSONResponse({"hubKey": user_cr.hub_key,
                             "hubId": str(user_cr.id).zfill(4),
                             })
    else:
        return JSONResponse({
             **ErrorCodes.get_error_response(1075)
        })


async def async_sleep(message):
    print("async sleep calls")
    for i in range(100):
        time.sleep(2)
        print(f"{message} :: {i}")
    return JSONResponse({"code": 100})


# @router.post("/test-async")
async def test_async_api(background_tasks: BackgroundTasks):
    start = time.time()
    futures = [async_sleep("Hi I am running in BG")]
    await asyncio.gather(*futures)
    end = time.time()
    print('It took {} seconds to finish execution.'.format(round(end - start)))
    # background_tasks.add_task(async_sleep, message="Hi Background")
    # print("Result",result)
    return {"code": 300}


@router.post("/test-async")
async def test_async_api():
    logger.info(f"testing async ")
    return {"code": 300}