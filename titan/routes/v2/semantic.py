from typing import List

from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorClient

from titan.auth import get_current_active_user
from titan.config import *
from titan.database import get_connection
from titan.models.user import UserInDB
from titan.semantic.bigowl import BIGOWL
from titan.semantic.repository import Virtuoso

router = APIRouter()
ontology = BIGOWL(Virtuoso(**settings.rdf_connection_settings))


@router.get("/component/get/all", name="List components from repository", tags=["semantic"])
async def all_components(include_parameters: bool = True, include_connections: bool = True) -> dict:
    """
    List all components from the repository with optional information about their connection and parameters.
    """
    return {
        **await ontology.components(
            "DataCollection", include_parameters=include_parameters, include_connections=include_connections
        ),
        **await ontology.components(
            "DataProcessing", include_parameters=include_parameters, include_connections=include_connections
        ),
        **await ontology.components(
            "DataAnalysing", include_parameters=include_parameters, include_connections=include_connections
        ),
        **await ontology.components(
            "DataSink", include_parameters=include_parameters, include_connections=include_connections
        ),
    }


@router.get("/component/parameters", name="Get component parameters", tags=["semantic"])
async def parameters(component_id: str, uri: str) -> list:
    return await ontology.parameters(uri=f"{uri}#{component_id}")


@router.get("/component/compatible", name="Get compatible components", tags=["semantic"])
async def compatible(component_id: str, uri: str) -> dict:
    return await ontology.compatible(uri=f"{uri}#{component_id}")


@router.get("/component/connection/inputs", name="Get input classes for given component", tags=["semantic"])
async def input_classes(component_id: str, uri: str) -> list:
    return await ontology.inputs(uri=f"{uri}#{component_id}")


@router.get("/component/connection/outputs", name="Get output classes for given component", tags=["semantic"])
async def output_classes(component_id: str, uri: str) -> list:
    return await ontology.outputs(uri=f"{uri}#{component_id}")


@router.get("/component/connection/parents", name="Get parent classes for given connection", tags=["semantic"])
async def parent_classes(connection_id: str, uri: str) -> List[str]:
    return await ontology.parents(uri=f"{uri}#{connection_id}")


@router.get(
    "/workflow/check/connections",
    name="Validate components interoperability in a workflow",
    tags=["semantic"],
)
async def invalid_components(workflow_id: str = None, uri: str = None) -> list:
    invalid = await ontology.check_invalid_components(uri=f"{uri}#{workflow_id}")
    return invalid


@router.post(
    "/mapping/new",
    name="Submit ontology mapping for given resource",
    tags=["semantic"],
)
async def submit_mapping(
    current_user: UserInDB = Depends(get_current_active_user),
    db: AsyncIOMotorClient = Depends(get_connection),
):
    # = await ontology.db.insert()
    pass
