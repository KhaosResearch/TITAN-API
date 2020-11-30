import uvicorn
from fastapi import Depends, FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.models import APIKey
from fastapi.openapi.utils import get_openapi
from starlette import status
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, Response

from titan import __version__
from titan.config import settings
from titan.database import close_db_connection, create_db_connection
from titan.routes.v2 import semantic, user
from titan.routes.v2 import workflow as w_v2
from titan.routes.v3 import workflow as w_v3
from titan.security import get_api_key

tags_metadata = [
    {
        "name": "semantic",
        "description": "",
    },
    {
        "name": "auth",
        "description": "Authentication methods.",
    },
    {
        "name": "user",
        "description": "User-related information.",
    },
    {
        "name": "workflow",
        "description": "Operations with workflows.",
    },
]

app = FastAPI(
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    openapi_tags=tags_metadata,
    root_path=settings.ROOT_PATH,
)


# cors settings

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# events

# app.add_event_handler("startup", configure_logging)
app.add_event_handler("startup", create_db_connection)

app.add_event_handler("shutdown", close_db_connection)


# api routes


@app.get("/api/health", name="Health check", status_code=status.HTTP_200_OK, tags=["health"])
async def health():
    return Response(status_code=status.HTTP_200_OK)


app.include_router(user.router, prefix="/api/v2/auth")
app.include_router(w_v2.router, prefix="/api/v2/workflow")
app.include_router(w_v3.router, prefix="/api/v3/workflow")
app.include_router(semantic.router, prefix="/api/v2/semantic")


# swagger documentation


@app.get("/api/openapi.json", tags=["documentation"])
async def get_open_api_endpoint(api_key: APIKey = Depends(get_api_key)):
    response = JSONResponse(
        get_openapi(
            title="TITAN API SERVICES",
            version=__version__,
            description="TITAN platform API services. https://github.com/benhid/titan-platform",
            routes=app.routes,
        )
    )
    return response


@app.get("/api/docs", tags=["documentation"])
async def get_documentation(api_key: APIKey = Depends(get_api_key)):
    response = get_swagger_ui_html(
        openapi_url=f"/api/openapi.json?{settings.API_KEY_NAME}={settings.API_KEY}",
        title="Documentation",
    )
    return response


def run_server():
    uvicorn.run(
        app,
        host=settings.API_HOST,
        port=settings.API_PORT,
        root_path=settings.ROOT_PATH,
        log_level="trace" if settings.API_DEBUG else "info",
    )
