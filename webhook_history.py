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
from schema import CheckWebhookHistorySchema
import views
from errors import ErrorCodes
from utils import get_financial_year, check_invoice_date, check_invoice_due_date
from utils import generate_key_secret, create_post_processing, validate_signature
from config import webhook_task
import datetime
from database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)


@router.post("/webhook_history/{merchant_key}")
def webhook_history(request: CheckWebhookHistorySchema,
                      db: Session = Depends(get_db)):

    try:
        wehbook_hist_ids = []
        from_date = datetime.datetime.strptime(request.fromDate, '%d/%m/%Y')
        to_date = datetime.datetime.strptime(request.toDate, '%d/%m/%Y')
        processing_obj = db.query(
            models.PostProcessingRequest
        ).filter(models.PostProcessingRequest.created_at.between(from_date, to_date)).all()
        for processing_data in processing_obj:
            # logger.info(f"getting loop part seller identifier {processing_data.request_extra_data.get('requestId')}")

            if processing_data.webhook_response:
                history_obj = {
                    "requestId": processing_data.request_extra_data.get('requestId'),
                    "webhookData": processing_data.webhook_response
                }
                if history_obj:
                    wehbook_hist_ids.append(history_obj)
        # logger.info(f"getting enquiry response webhook data >>>>>>>>>>>>>>>> {wehbook_hist_ids}")

        return {
            "requestId": request.requestId,
            **ErrorCodes.get_error_response(200),
            "webhookHistory": wehbook_hist_ids
        }
    except Exception as e:
        logger.error(f"getting error while entity registration {e}")
        return {**ErrorCodes.get_error_response(500)}
