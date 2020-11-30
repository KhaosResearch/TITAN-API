from fastapi.testclient import TestClient
from starlette.status import HTTP_401_UNAUTHORIZED

from titan.app import app

client = TestClient(app)


def test_cannot_create_new_workflow_if_unauthenticated():
    response = client.post("/api/v2/workflow/new")
    assert response.status_code == HTTP_401_UNAUTHORIZED
    assert response.json() == {"detail": "Not authenticated"}
