import logging

from fastapi import APIRouter
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi import FastAPI, Request
from starlette.responses import FileResponse

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="templates")
class TagsMetadata:
    simulator_dict = {
        "name": "",
        "description": "**Aim** : This is Test Ui to demonstrate each api by providing merchant key and request data."
    }
    simulator_history_dict = {
        "name": "",
        "description": "**Aim** : This UI list the history which is maintained on each  api call via simulator."
    }

@router.get("/", response_class=HTMLResponse, description=TagsMetadata.simulator_dict.get('description'))
async def index(request: Request):
    return templates.TemplateResponse(
        name="simulator/index.html",
        context={
            'request': request
        }
    )


@router.get("/history", response_class=HTMLResponse,  description=TagsMetadata.simulator_dict.get('description'))
async def index(request: Request):
    return templates.TemplateResponse(
        name="simulator/history.html",
        context={
            'request': request,
        }
    )

# from fastapi import FastAPI
# from fastapi.openapi.models import OAuthFlows
# from fastapi.openapi.models import OAuthFlowAuthorizationCode

# app = FastAPI()

# Serve the custom Swagger UI HTML page
# @router.get("/docs", include_in_schema=False)
# def custom_swagger_ui():
#     return FileResponse("swagger_ui.html")
#
# # Use the custom Swagger UI HTML page for the openapi_url
# router.openapi_url = "simulator/docs/openapi.json"

# @router.get("/docs", response_class=HTMLResponse,  description=TagsMetadata.simulator_dict.get('description'))
# async def index(request: Request):
#     return templates.TemplateResponse(
#         name="simulator/history.html",
#         context={
#             'request': request,
#         }
#     )