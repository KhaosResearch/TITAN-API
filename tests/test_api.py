from fastapi.testclient import TestClient
from starlette.status import HTTP_200_OK, HTTP_403_FORBIDDEN

from titan.app import app
from titan.config import settings

client = TestClient(app)


def test_health():
    response = client.get("/api/health")
    assert response.status_code == HTTP_200_OK


def test_cannot_get_documentation_if_invalid_token():
    response = client.get("/api/openapi.json")
    assert response.status_code == HTTP_403_FORBIDDEN
    assert response.json() == {"detail": "Invalid access token"}


def test_can_get_documentation_if_token_in_header():
    response = client.get("/api/openapi.json", headers={settings.API_KEY_NAME: settings.API_KEY})
    assert response.status_code == HTTP_200_OK


def test_can_get_documentation_if_token_in_query():
    response = client.get(f"/api/openapi.json?{settings.API_KEY_NAME}={settings.API_KEY}")
    assert response.status_code == HTTP_200_OK


def test_can_get_documentation_if_token_in_cookies():
    response = client.get(f"/api/openapi.json", cookies={settings.API_KEY_NAME: settings.API_KEY})
    assert response.status_code == HTTP_200_OK
