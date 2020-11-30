import collections
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import rdflib
import strconv
from pydantic import BaseModel
from rdflib import RDF, Graph, Literal

from titan.logger import get_logger

logger = get_logger(__name__)


# Task


class State(str, Enum):
    STATUS_UNKNOWN: str = "UNKNOWN"
    STATUS_REVOKED: str = "REVOKED"
    STATUS_PENDING: str = "PENDING"
    STATUS_RUNNING: str = "RUNNING"
    STATUS_FAILED: str = "FAILED"
    STATUS_DONE: str = "DONE"


class Task(BaseModel):
    name: str
    params: dict = {}
    inputs: dict = {}
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    result: Optional[dict] = None
    status: State = None

    class Config:
        use_enum_values = True


# Workflow


class Properties(BaseModel):
    name: str

    class Config:
        extra = "allow"


class ParameterProperties(Properties):
    value: Any
    defaultValue: Any

    class Config:
        extra = "allow"


class Definition(BaseModel):
    uri: str

    class Config:
        extra = "allow"


class InputOutput(BaseModel):
    properties: Properties
    definition: Optional[Definition]

    class Config:
        extra = "allow"


class OperatorProperties(BaseModel):
    name: str
    module: str
    inputs: Dict[str, InputOutput] = {}
    outputs: Dict[str, InputOutput] = {}

    class Config:
        extra = "allow"


class OperatorParameters(BaseModel):
    properties: ParameterProperties
    definition: Optional[Definition]

    class Config:
        extra = "allow"


class Operator(BaseModel):
    properties: OperatorProperties
    parameters: Dict[str, OperatorParameters] = {}
    definition: Optional[Definition]

    class Config:
        extra = "allow"


class Link(BaseModel):
    fromOperator: Union[str, int]
    fromConnector: Union[str, int]
    toOperator: Union[str, int]
    toConnector: Union[str, int]

    class Config:
        extra = "allow"


class WorkflowRequest(BaseModel):
    """
    This class represents the bare-minimum information requited to execute a workflow.
    """

    operators: Dict[str, Operator] = None
    links: Dict[str, Link] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    metadata: dict = {}


class WorkflowInDB(WorkflowRequest):
    """
    Workflow represented in database.
    """

    id: str = ""
    executed: Optional[str] = None  # last execution id


class WorkflowInDBWithStatus(WorkflowInDB):
    """
    Workflow with execution status.
    """

    tasks: Optional[List[Task]] = None
    status: State = State.STATUS_UNKNOWN


# Search results


class Pagination(BaseModel):
    page_size: int  # No of elements in page
    page_num: int  # current page
    page_count: int  # No of pages
    total_count: int  # No of elements in all pages


class WorkflowSearchResult(BaseModel):
    """
    Database search results.
    """

    workflows: List[WorkflowInDB]
    pagination: Pagination


class WorkflowStatusSearchResult(BaseModel):
    """
    Database search results.
    """

    workflows: List[WorkflowInDBWithStatus]
    pagination: Pagination


# Parsing methods


def parse_to_list(workflow) -> list:
    """
    Split a JSON-formatted workflow into a list of tasks.
    """
    links = collections.defaultdict(dict)
    _links = workflow["links"]

    for link in _links.values():
        to_op_idx = link["toOperator"]
        to_con_idx = link["toConnector"]

        from_op_idx = link["fromOperator"]
        from_con_idx = link["fromConnector"]

        logger.debug(f"found link {from_op_idx}-{from_con_idx} -> {to_op_idx}-{to_con_idx}")

        links[to_op_idx][to_con_idx] = (from_op_idx, from_con_idx)

    logger.debug(f"links {links}")

    tasks = []
    _operators = workflow["operators"]

    for op_idx, op_data in _operators.items():
        name = op_data["properties"]["name"]
        module = op_data["properties"]["module"]
        params = {}

        for param_idx, param_data in op_data["parameters"].items():
            param_properties = param_data["properties"]
            param_name = param_properties["name"]
            param_value = param_properties["value"]

            try:
                param_value = strconv.convert(param_value)
            except ValueError:
                pass

            params[param_name] = param_value

        inputs = {}

        for in_idx, in_data in op_data["properties"]["inputs"].items():
            in_properties = in_data["properties"]
            in_name = in_properties["name"]

            try:
                from_op, from_conn = links[op_idx][in_idx]

                from_op_name = workflow["operators"][from_op]["properties"]["name"]
                from_op_con_name = workflow["operators"][from_op]["properties"]["outputs"][from_conn]

                inputs[in_name] = f"{from_op_name}.{from_op_con_name['properties']['name']}"
            except KeyError:
                inputs[in_name] = None

        tasks.append({"name": name, "module": module, "params": params, "inputs": inputs})

    logger.debug(f"tasks {tasks}")

    return tasks


def parse_to_rdf(workflow, workflow_id: str) -> Graph:
    """
    Transform a JSON-formatted workflow to RDF n-triples.
    """
    PREFIX_TITAN = "http://www.ontologies.khaos.uma.es/titan/#"
    PREFIX_BIGOWL = "http://www.ontologies.khaos.uma.es/bigowl#"
    PREFIX_DMOP = "http://www.e-lico.eu/ontologies/dmo/DMOP/DMOP.owl#"

    # workflow related
    uriref_workflow = rdflib.URIRef(PREFIX_DMOP + "Workflow")

    # task related
    uriref_num_task = rdflib.URIRef(PREFIX_BIGOWL + "numTask")
    uriref_has_task = rdflib.URIRef(PREFIX_BIGOWL + "hasTask")

    uriref_task_has_number_outputs = rdflib.URIRef(PREFIX_BIGOWL + "numberOfOutputs")
    uriref_task_has_number_inputs = rdflib.URIRef(PREFIX_BIGOWL + "numberOfInputs")

    uriref_task_specifies_input_class = rdflib.URIRef(PREFIX_BIGOWL + "specifiesInputClass")
    uriref_task_specifies_output_class = rdflib.URIRef(PREFIX_BIGOWL + "specifiesOutputClass")

    uriref_has_task_name = rdflib.URIRef(PREFIX_BIGOWL + "hasName")

    # parameter related
    uriref_has_parameter = rdflib.URIRef(PREFIX_BIGOWL + "hasParameter")

    # component related
    uriref_has_component = rdflib.URIRef(PREFIX_BIGOWL + "hasComponent")

    g = Graph()

    name = rdflib.URIRef(PREFIX_TITAN + "Workflow" + workflow_id)
    g.add((name, RDF.type, uriref_workflow))
    g.add((name, uriref_num_task, Literal(len(workflow["operators"]))))

    for op_idx, op_data in workflow["operators"].items():
        task = rdflib.URIRef(op_data["definition"]["uri"] + f"-{workflow_id}-{op_idx}")
        g.add((name, uriref_has_task, task))

        task_name = op_data["properties"]["name"]
        g.add((task, uriref_has_task_name, Literal(task_name)))

        # inputs
        g.add(
            (
                task,
                uriref_task_has_number_inputs,
                Literal(len(op_data["properties"]["inputs"])),
            )
        )
        for _, op_input in op_data["properties"]["inputs"].items():
            input_task = rdflib.URIRef(op_input["definition"]["uri"])
            g.add((task, uriref_task_specifies_input_class, input_task))

        # outputs
        g.add(
            (
                task,
                uriref_task_has_number_outputs,
                Literal(len(op_data["properties"]["outputs"])),
            )
        )
        for _, op_output in op_data["properties"]["outputs"].items():
            output_task = rdflib.URIRef(op_output["definition"]["uri"])
            g.add((task, uriref_task_specifies_output_class, output_task))

        # component
        component = rdflib.URIRef(op_data["definition"]["uri"])
        g.add((task, uriref_has_component, component))

    return g
