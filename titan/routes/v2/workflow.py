from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient

from titan.auth import get_current_active_user
from titan.database import get_connection
from titan.logger import get_logger
from titan.manager import WorkflowManager
from titan.models.user import UserInDB
from titan.models.workflow import (
    State,
    WorkflowInDB,
    WorkflowInDBWithStatus,
    WorkflowRequest,
    WorkflowSearchResult,
)

logger = get_logger(__name__)

router = APIRouter()


@router.post(
    "/new", name="Create new workflow in database", tags=["workflow"], response_model=WorkflowInDB, status_code=200
)
async def new(
    workflow: WorkflowRequest = WorkflowRequest(),
    current_user: UserInDB = Depends(get_current_active_user),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowInDB:
    """
    Creates empty workflow in database.
    """
    workflow = await WorkflowManager().insert(db, username=current_user.username, workflow=workflow)
    return workflow


@router.post(
    "/update",
    name="Update workflow in database",
    tags=["workflow"],
    responses={
        404: {"description": "Workflow not found"},
    },
    response_model=WorkflowInDB,
    status_code=200,
)
async def update(
    workflow_id: str,
    workflow: WorkflowRequest,
    current_user: UserInDB = Depends(get_current_active_user),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowInDB:
    """
    Updates existing workflow in database.
    """
    exists = await WorkflowManager().find_one(db, username=current_user.username, workflow_id=workflow_id)
    if not exists:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    workflow = await WorkflowManager().upsert(
        db, username=current_user.username, workflow_id=workflow_id, workflow=workflow
    )

    return workflow


@router.get(
    "/get",
    name="Get workflow from database",
    tags=["workflow"],
    responses={
        404: {"description": "Workflow not found"},
    },
    response_model=WorkflowInDB,
    status_code=200,
)
async def get(
    workflow_id: str,
    current_user: UserInDB = Depends(get_current_active_user),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowInDB:
    """
    Retrieves data from workflow in database.
    """
    workflow = await WorkflowManager().find_one(db, username=current_user.username, workflow_id=workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
    return workflow


@router.get(
    "/get/all", name="List user's workflows", tags=["workflow"], response_model=WorkflowSearchResult, status_code=200
)
async def get_all(
    page_size: int = Query(default=10, ge=1),
    page_num: int = Query(default=1, ge=1),
    current_user: UserInDB = Depends(get_current_active_user),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowSearchResult:
    """
    Lists all workflows from current user.
    """
    workflows, total_count = await WorkflowManager().find(
        db, username=current_user.username, page_size=page_size, page_num=page_num
    )

    return WorkflowSearchResult(
        workflows=workflows,
        pagination={
            "page_size": page_size,
            "page_num": page_num,
            "page_count": round(total_count / page_size),
            "total_count": total_count,
        },
    )


@router.post(
    "/run",
    name="Run workflow",
    tags=["workflow"],
    responses={
        404: {"description": "Workflow not found"},
        500: {"description": "Missing key"},
    },
    response_model=WorkflowInDB,
    status_code=200,
)
async def run(
    workflow_id: str,
    current_user: UserInDB = Depends(get_current_active_user),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowInDB:
    """
    Executes workflow from database.
    """
    # get workflow from db
    workflow = await WorkflowManager().find_one(db, username=current_user.username, workflow_id=workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found")

    # execute
    try:
        await WorkflowManager().execute(db, workflow=workflow)
    except KeyError as err:
        logger.debug(f"There was an error executing the workflow '{workflow_id}'")
        raise HTTPException(status_code=500, detail=f"Missing key '{err.args[0]}'")

    workflow = await WorkflowManager().find_one(db, username=current_user.username, workflow_id=workflow_id)

    return workflow


@router.post(
    "/revoke",
    name="Revoke workflow execution",
    tags=["workflow"],
    responses={
        404: {"description": "Workflow not found"},
        500: {"description": "Workflow has not been executed yet"},
    },
    status_code=200,
)
async def revoke(
    workflow_id: str,
    current_user: UserInDB = Depends(get_current_active_user),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> None:
    """
    Revoke workflow execution.
    """
    # get workflow from db
    workflow = await WorkflowManager().find_one(db, username=current_user.username, workflow_id=workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    # check status
    if not workflow.executed:
        raise HTTPException(status_code=404, detail=f"Workflow has not been executed yet")

    await WorkflowManager().revoke(workflow)


@router.get(
    "/status",
    name="Get workflow execution status",
    tags=["workflow"],
    responses={
        404: {"description": "Workflow not found"},
        500: {"description": "Workflow has not been executed yet"},
    },
    response_model=WorkflowInDBWithStatus,
    response_model_exclude={"operators", "links"},
    status_code=200,
)
async def status(
    workflow_id: str,
    current_user: UserInDB = Depends(get_current_active_user),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowInDBWithStatus:
    """
    Checks execution status from workflow in database.
    """
    # get workflow from db
    workflow = await WorkflowManager().find_one(db, username=current_user.username, workflow_id=workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    # check status
    if not workflow.executed:
        raise HTTPException(status_code=404, detail=f"Workflow has not been executed yet")

    response, status_code = await WorkflowManager().status(workflow)
    if status_code != 200:
        raise HTTPException(status_code=500, detail="Could not retrieve workflow status")

    # read tasks
    tasks_with_status = []
    tasks_status_only = []

    for task in response["tasks"]:
        _status = task.get("status").upper()  # compatibility with older DRAMA versions
        tasks_with_status.append(
            {
                "name": task.get("name"),
                "params": task.get("params"),
                "inputs": task.get("inputs"),
                "created_at": task.get("created_at"),
                "updated_at": task.get("updated_at"),
                "result": task.get("result"),
                "status": _status,
            }
        )
        tasks_status_only.append(_status)

    # append global status based on task statuses
    def _check(comp: Callable, stats: list) -> bool:
        return comp([s in stats for s in tasks_status_only])

    if _check(all, [State.STATUS_DONE]):
        workflow_status = State.STATUS_DONE
    elif _check(all, [State.STATUS_PENDING]):
        workflow_status = State.STATUS_PENDING
    elif _check(all, [State.STATUS_PENDING, State.STATUS_RUNNING]):
        workflow_status = State.STATUS_RUNNING
    elif _check(any, [State.STATUS_FAILED]):
        workflow_status = State.STATUS_FAILED
    else:
        workflow_status = State.STATUS_UNKNOWN

    workflow_with_status = WorkflowInDBWithStatus(**workflow.dict(), tasks=tasks_with_status, status=workflow_status)

    return workflow_with_status
