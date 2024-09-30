from fastapi import APIRouter

router = APIRouter()

@router.post("/")
def check_health():
    logger.info(f"health check  called")


    return {"code":  200}
