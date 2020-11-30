import math
import traceback
from typing import Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND

from titan.auth import get_user_by_username
from titan.database import get_connection
from titan.logger import get_logger
from titan.manager import WorkflowManager
from titan.models.workflow import (
    State,
    Task,
    WorkflowInDB,
    WorkflowInDBWithStatus,
    WorkflowRequest,
    WorkflowSearchResult,
    WorkflowStatusSearchResult,
)

logger = get_logger(__name__)

router = APIRouter()


@router.post(
    "/new",
    summary="Creates new workflow",
    tags=["workflow"],
    response_model=WorkflowInDB,
    response_description="Workflow from database with associated metadata",
    status_code=201,
)
async def new(
    username: str,
    workflow: WorkflowRequest = WorkflowRequest(),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowInDB:
    """
    Creates new workflow in database.

    If workflow is specified, inserts workflow in database instead.
    """
    user_by_username = await get_user_by_username(db, username)
    if not user_by_username:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Username not found")

    workflow = await WorkflowManager().insert(db, username=username, workflow=workflow)

    return workflow


@router.post(
    "/update",
    summary="Updates workflow with new content",
    tags=["workflow"],
    responses={
        404: {"description": "Workflow not found"},
    },
    response_model=WorkflowInDB,
    response_description="Workflow from database with associated metadata",
    status_code=201,
)
async def update(
    username: str,
    workflow_id: str,
    workflow: WorkflowRequest,
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowInDB:
    """
    Updates existing workflow in database.
    """
    user_by_username = await get_user_by_username(db, username)
    if not user_by_username:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Username not found")

    exists = await WorkflowManager().find_one(db, username=username, workflow_id=workflow_id)
    if not exists:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    workflow = await WorkflowManager().upsert(db, username=username, workflow_id=workflow_id, workflow=workflow)

    return workflow


def _exclude_keys(dictionary, keys: list):
    """Filters a dict by excluding certain keys."""
    key_set = set(dictionary.keys()) - set(keys)
    return {key: dictionary[key] for key in key_set}


@router.get(
    "/get",
    summary="Gets workflow(s)",
    tags=["workflow"],
    responses={404: {"description": "No results matching query were found"}},
    response_model=WorkflowSearchResult,
    response_description="Search result",
    status_code=200,
)
async def get(
    request: Request,
    username: str,
    workflow_id: Optional[str] = None,
    page_size: int = Query(default=1, ge=1),
    page_num: int = Query(default=1, ge=1),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowSearchResult:
    """
    Retrieves workflows from database.

    This endpoint allows an arbitrary number of optional query parameters for filtering purposes, e.g.:

    ```?username=test&page_size=1&page_num=1&metadata.key=value&metadata.key2=value2```
    """
    user_by_username = await get_user_by_username(db, username)
    if not user_by_username:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Username not found")

    if workflow_id:
        workflows, total_count = await WorkflowManager().find(
            db, username=username, id=workflow_id, page_size=page_size, page_num=page_num
        )
    else:
        query_params = request.query_params
        filtering = _exclude_keys(query_params, ["username", "workflow_id", "page_size", "page_num"])

        workflows, total_count = await WorkflowManager().find(
            db, username=username, page_size=page_size, page_num=page_num, **filtering
        )

    if not workflows:
        raise HTTPException(status_code=404, detail="No results matching query were found")

    return WorkflowSearchResult(
        workflows=workflows,
        pagination={
            "page_size": len(workflows),
            "page_num": page_num,
            "page_count": math.ceil(total_count / page_size),
            "total_count": total_count,
        },
    )


@router.get(
    "/status",
    summary="Gets workflow(s) execution states",
    tags=["workflow"],
    responses={404: {"description": "No results matching query were found"}},
    response_model=WorkflowStatusSearchResult,
    response_description="Search result",
    status_code=200,
)
async def status(
    request: Request,
    username: str,
    workflow_id: Optional[str] = None,
    page_size: int = Query(default=1, ge=1),
    page_num: int = Query(default=1, ge=1),
    exclude_key: list = Query(default=["operators", "links"]),
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowStatusSearchResult:
    """
    Retrieves workflows from database including its execution status.

    This endpoint allows an arbitrary number of optional query parameters for filtering purposes, e.g.:

    ```?username=test&page_size=1&page_num=1&metadata.key=value&metadata.key2=value2```
    """
    user_by_username = await get_user_by_username(db, username)
    if not user_by_username:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Username not found")

    if workflow_id:
        workflows, total_count = await WorkflowManager().find(
            db, username=username, id=workflow_id, page_size=page_size, page_num=page_num
        )
    else:
        query_params = request.query_params
        filtering = _exclude_keys(query_params, ["username", "workflow_id", "page_size", "page_num", "exclude_key"])

        workflows, total_count = await WorkflowManager().find(
            db, username=username, page_size=page_size, page_num=page_num, **filtering
        )

    if not workflows:
        raise HTTPException(status_code=404, detail="No results matching query were found")

    workflows_with_status = []

    for workflow in workflows:
        workflow_as_dict = workflow.dict(exclude=set(exclude_key))

        workflow_with_status = WorkflowInDBWithStatus(**workflow_as_dict, tasks=None, status=State.STATUS_UNKNOWN)

        # get tasks statuses from workflow
        # and derive global status
        try:
            assert workflow.executed, "Workflow has not been executed yet"

            # fetch tasks' statuses
            response, status_code = await WorkflowManager().status(workflow)
            assert status_code, "Could not establish connection with database"
            assert status_code == 200, "Status request failed"

            # read tasks
            tasks_with_status = []
            tasks_statuses_only = []

            for task in response["tasks"]:
                tasks_with_status.append(Task(**task))

                task_status = task.get("status").upper()  # compatibility with older DRAMA versions
                tasks_statuses_only.append(task_status)

            # append global status based on task statuses
            def _check(comp: Callable, stats: list) -> bool:
                return comp([s in stats for s in tasks_statuses_only])

            # check global status
            if response.get("is_revoked"):
                workflow_status = State.STATUS_REVOKED
            elif _check(all, [State.STATUS_DONE]):
                workflow_status = State.STATUS_DONE
            elif _check(any, [State.STATUS_FAILED]):
                workflow_status = State.STATUS_FAILED
            elif _check(all, [State.STATUS_PENDING]):
                workflow_status = State.STATUS_PENDING
            elif _check(any, [State.STATUS_PENDING]) and not _check(any, [State.STATUS_FAILED]):
                workflow_status = State.STATUS_PENDING
            elif _check(any, [State.STATUS_RUNNING]) and not _check(any, [State.STATUS_FAILED]):
                workflow_status = State.STATUS_RUNNING
            else:
                workflow_status = State.STATUS_UNKNOWN

            workflow_with_status = WorkflowInDBWithStatus(
                **workflow_as_dict, tasks=tasks_with_status, status=workflow_status
            )
        except Exception:
            logger.error(traceback.format_exc())

        workflows_with_status.append(workflow_with_status)

    return WorkflowStatusSearchResult(
        workflows=workflows_with_status,
        pagination={
            "page_size": len(workflows_with_status),
            "page_num": page_num,
            "page_count": math.ceil(total_count / page_size),
            "total_count": total_count,
        },
    )


@router.get(
    "/fstatus",
    summary="Gets workflow(s) execution states",
    tags=["workflow", "dev"],
    responses={404: {"description": "No results matching query were found"}},
    response_model=WorkflowStatusSearchResult,
    response_description="Search result",
    status_code=200,
)
async def status(
    request: Request,
    username: str,
    page_size: int = Query(default=1, ge=1),
    page_num: int = Query(default=1, ge=1),
    exclude_key: list = Query(default=["operators", "links"]),
    with_status: State = State.STATUS_DONE,
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowStatusSearchResult:
    user_by_username = await get_user_by_username(db, username)
    if not user_by_username:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Username not found")

    query_params = request.query_params
    filtering = _exclude_keys(query_params, ["username", "page_size", "page_num", "exclude_key", "with_status"])

    workflows = WorkflowManager().find_all(db, username=username, **filtering)

    current_count, total_count = 0, 0
    skips = page_size * (page_num - 1)

    workflows_with_status = []

    async for workflow in workflows:
        workflow_as_dict = workflow.dict(exclude=set(exclude_key))

        workflow_with_status = WorkflowInDBWithStatus(**workflow_as_dict, tasks=None, status=State.STATUS_UNKNOWN)

        # get tasks statuses from workflow
        # and derive global status
        try:
            assert workflow.executed, "Workflow has not been executed yet"

            # fetch tasks' statuses
            response, status_code = await WorkflowManager().status(workflow)
            assert status_code, "Could not establish connection with database"
            assert status_code == 200, "Status request failed"

            # read tasks
            tasks_with_status = []
            tasks_statuses_only = []

            for task in response["tasks"]:
                tasks_with_status.append(Task(**task))

                task_status = task.get("status").upper()  # compatibility with older DRAMA versions
                tasks_statuses_only.append(task_status)

            # append global status based on task statuses
            def _check(comp: Callable, stats: list) -> bool:
                return comp([s in stats for s in tasks_statuses_only])

            # check global status
            if response.get("is_revoked"):
                workflow_status = State.STATUS_REVOKED
            elif _check(all, [State.STATUS_DONE]):
                workflow_status = State.STATUS_DONE
            elif _check(any, [State.STATUS_FAILED]):
                workflow_status = State.STATUS_FAILED
            elif _check(all, [State.STATUS_PENDING]):
                workflow_status = State.STATUS_PENDING
            elif _check(any, [State.STATUS_PENDING]) and not _check(any, [State.STATUS_FAILED]):
                workflow_status = State.STATUS_PENDING
            elif _check(any, [State.STATUS_RUNNING]) and not _check(any, [State.STATUS_FAILED]):
                workflow_status = State.STATUS_RUNNING
            else:
                workflow_status = State.STATUS_UNKNOWN

            workflow_with_status = WorkflowInDBWithStatus(
                **workflow_as_dict, tasks=tasks_with_status, status=workflow_status
            )
        except Exception:
            logger.error(traceback.format_exc())

        if workflow_with_status.status == with_status:
            if total_count >= skips and len(workflows_with_status) < page_size:
                current_count += 1
                workflows_with_status.append(workflow_with_status)
            total_count += 1

        # if page_size <= len(workflows_with_status):
        #    break

    if not workflows_with_status:
        raise HTTPException(status_code=404, detail="No results matching query were found")

    return WorkflowStatusSearchResult(
        workflows=workflows_with_status,
        pagination={
            "page_size": len(workflows_with_status),
            "page_num": page_num,
            "page_count": math.ceil(total_count / page_size),
            "total_count": total_count,
        },
    )


@router.post(
    "/run",
    summary="Executes workflow",
    tags=["workflow"],
    responses={
        404: {"description": "Workflow not found"},
        500: {"description": "Missing key"},
    },
    response_model=WorkflowInDB,
    response_description="Workflow from database with associated metadata",
    status_code=200,
)
async def run(
    username: str,
    workflow_id: str,
    db: AsyncIOMotorClient = Depends(get_connection),
) -> WorkflowInDB:
    """
    Executes workflow from database.
    """
    user_by_username = await get_user_by_username(db, username)
    if not user_by_username:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Username not found")

    # get workflow from db
    workflow = await WorkflowManager().find_one(db, username=username, workflow_id=workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found")

    # execute
    try:
        await WorkflowManager().execute(db, workflow=workflow)
    except KeyError as err:
        logger.debug(f"There was an error executing the workflow '{workflow_id}'")
        raise HTTPException(status_code=500, detail=f"Missing key '{err.args[0]}'")

    workflow = await WorkflowManager().find_one(db, username=username, workflow_id=workflow_id)

    return workflow


@router.post(
    "/revoke",
    summary="Revokes workflow execution",
    tags=["workflow"],
    responses={
        404: {"description": "Workflow not found"},
        500: {"description": "Workflow has not been executed yet"},
    },
    status_code=200,
)
async def revoke(
    username: str,
    workflow_id: str,
    db: AsyncIOMotorClient = Depends(get_connection),
) -> None:
    """
    Revoke workflow execution.
    """
    user_by_username = await get_user_by_username(db, username)
    if not user_by_username:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Username not found")

    # get workflow from db
    workflow = await WorkflowManager().find_one(db, username=username, workflow_id=workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    # check status
    if not workflow.executed:
        raise HTTPException(status_code=404, detail=f"Workflow has not been executed yet")

    await WorkflowManager().revoke(workflow)
