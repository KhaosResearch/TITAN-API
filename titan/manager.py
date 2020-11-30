import json
import uuid
from datetime import datetime
from typing import List, Optional, Tuple

import httpx
from motor.motor_asyncio import AsyncIOMotorClient

from titan.config import settings
from titan.logger import get_logger
from titan.models.workflow import (
    WorkflowInDB,
    WorkflowRequest,
    parse_to_list,
    parse_to_rdf,
)
from titan.semantic.repository import Virtuoso

logger = get_logger(__name__)


class _DramaAsyncClient:

    api_endpoint = f"http://{settings.DRAMA_HOST}:{settings.DRAMA_PORT}"
    api_token = settings.DRAMA_TOKEN

    @classmethod
    async def get(cls, resource: str) -> (dict, int):
        logger.debug(f"Accessing {cls.api_endpoint}{resource}")
        async with httpx.AsyncClient() as client:
            req = await client.get(f"{cls.api_endpoint}{resource}", headers={"x-token": cls.api_token})
        res, status = {}, req.status_code
        if not req.is_error:
            res = req.json()
        return res, status

    @classmethod
    async def post(cls, resource: str, data: dict = None) -> (dict, int):
        def serializer(o):
            if isinstance(o, datetime):
                return o.isoformat()

        async with httpx.AsyncClient() as client:
            req = await client.post(
                f"{cls.api_endpoint}{resource}",
                headers={"x-token": cls.api_token},
                data=json.dumps(data, default=serializer) if data else None,
            )
        res, status = {}, req.status_code
        if not req.is_error:
            res = req.json()
        return res, status


class WorkflowManager:
    async def execute(self, db: AsyncIOMotorClient, workflow: WorkflowInDB) -> Tuple[dict, int]:
        """
        :param db: Database client connection.
        :param workflow: Workflow stored in database.
        """
        workflow_as_dict = workflow.dict()

        try:
            tasks = parse_to_list(workflow_as_dict)
            logger.debug(tasks)
        except KeyError:
            logger.exception(f"Could not parse workflow")
            raise

        req, status = await _DramaAsyncClient.post(
            "/api/v2/workflow/run",
            data={
                "tasks": tasks,
                "metadata": {**workflow_as_dict["metadata"]},
            },
        )

        logger.debug(req)

        if status == 200:
            # update last execution id
            execution_id = req["id"]
            await db.workflow.update_one({"id": workflow.id}, {"$set": {"executed": execution_id}}, upsert=True)

        return req, status

    async def revoke(self, workflow: WorkflowInDB) -> Tuple[dict, int]:
        """
        :param db: Database client connection.
        :param workflow: Workflow stored in database.
        """
        last_exec_id = workflow.executed
        req, status = await _DramaAsyncClient.post(f"/api/v2/workflow/revoke?id={last_exec_id}")
        return req, status

    async def status(self, workflow: WorkflowInDB) -> Tuple[dict, int]:
        """
        :param workflow: Workflow stored in database.
        """
        last_exec_id = workflow.executed
        req, status = await _DramaAsyncClient.get(f"/api/v2/workflow/status?id={last_exec_id}")
        return req, status

    async def insert(
        self,
        db: AsyncIOMotorClient,
        username: str,
        workflow: WorkflowRequest,
        workflow_id: Optional[str] = None,
    ) -> WorkflowInDB:
        """
        Inserts a new workflow in database. A random unique id will be automatically generated if not provided.

        :param db: Database client connection.
        :param username: Owner/user.
        :param workflow: Workflow request from user.
        :param workflow_id: Unique workflow identifier.
        """
        if not workflow_id:
            workflow_id = str(uuid.uuid4().hex)
        workflow.created_at = datetime.now().isoformat()
        return await self.upsert(db, username, workflow_id, workflow)

    async def upsert(
        self, db: AsyncIOMotorClient, username: str, workflow_id: str, workflow: WorkflowRequest
    ) -> WorkflowInDB:
        """
        Inserts or updates workflow in database.

        :param db: Database client connection.
        :param username: Owner/user.
        :param workflow_id: Unique workflow identifier.
        :param workflow: Workflow request from user.
        """
        workflow.updated_at = datetime.now().isoformat()

        # append some metadata
        workflow_as_dict = workflow.dict(exclude_none=True)
        workflow_as_dict["metadata"]["author"] = username

        # upsert -> if the record does not exist, insert it
        await db.workflow.update_one({"id": workflow_id}, {"$set": workflow_as_dict}, upsert=True)

        # insert rdf to repository
        try:
            workflow_rdf = parse_to_rdf(workflow_as_dict, workflow_id=workflow_id)
            workflow_rdf_as_nt = workflow_rdf.serialize(format="nt")

            logger.debug(f"Workflow {workflow_id} correctly parsed as {workflow_rdf_as_nt}")

            triples = str(workflow_rdf_as_nt.decode("UTF-8"))

            store = Virtuoso(**settings.rdf_connection_settings)
            query = "INSERT DATA { GRAPH <" + store.database + "> {" + triples + "} }"

            await store.update(query)
        except Exception as err:
            logger.exception(f"Could not store workflow's RDF: {err.args[0]}")

        return WorkflowInDB(id=workflow_id, **workflow_as_dict)

    async def find_one(self, db: AsyncIOMotorClient, username: str, workflow_id: str) -> WorkflowInDB:
        """
        Find workflow in database from current user given its id.

        :param db: Database client connection.
        :param username: Owner/user.
        :param workflow_id: Unique workflow id.
        """
        workflow = await db.workflow.find_one({"id": workflow_id, "metadata.author": username}, {"_id": 0})
        if workflow:
            return WorkflowInDB(**workflow)

    async def find_all(self, db: AsyncIOMotorClient, username: str, **kwargs):
        """
        Find workflows in database.

        :param db: Database client connection.
        :param username: Owner/user.
        """
        cursor = db.workflow.find({"metadata.author": username, **kwargs}, {"_id": 0})
        cursor.sort([("updated_at", -1)])
        async for document in cursor:
            yield WorkflowInDB(**document)

    async def find(
        self, db: AsyncIOMotorClient, username: str, page_size: int, page_num: int, **kwargs
    ) -> Tuple[List[WorkflowInDB], int]:
        """
        Find workflows in database.

        :param db: Database client connection.
        :param username: Owner/user.
        :param page_size: Number of documents to return.
        :param page_num: Page number for document's pagination.
        """
        # calculate number of documents to skip
        skips = page_size * (page_num - 1)
        # skip and limit
        workflows = (
            db.workflow.find({"metadata.author": username, **kwargs}, {"_id": 0})
            .sort([("updated_at", -1)])
            .skip(skips)
            .limit(page_size)
        )
        workflows = await workflows.to_list(length=page_size)
        # count total
        count = db.workflow.count_documents({"metadata.author": username, **kwargs})
        # returns model
        return [WorkflowInDB(**workflow) for workflow in workflows], await count
