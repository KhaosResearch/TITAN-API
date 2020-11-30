from typing import List

from aiocache import cached

from titan.logger import get_logger
from titan.semantic.repository import RDFRepository

logger = get_logger(__name__)


def get_name(uri: str) -> str:
    """
    Splits an URI and returns the individual name.
    """
    return uri.split("/")[-1]


class BIGOWLQueries:
    GET_COMPONENTS = """
        SELECT DISTINCT ?individual ?type ?label ?description ?ninputs ?noutputs
        WHERE {{
            ?individual rdf:type ?type. 
            ?type rdfs:subClassOf* <http://www.ontologies.khaos.uma.es/bigowl/{type}>. 
            OPTIONAL {{ ?individual rdfs:label ?label . }} .
            OPTIONAL {{ ?individual rdfs:comment ?description . }} .
            OPTIONAL {{ ?individual bigowl:numberOfInputs ?ninputs. }} .
            OPTIONAL {{ ?individual bigowl:numberOfOutputs ?noutputs. }}
        }} 
    """
    GET_PARAMETERS = """
        SELECT ?param ?type ?label ?name ?range ?defaultValue
        WHERE {{
            <{component}> bigowl:hasParameter ?param .
            ?param bigowl:hasDataType ?type .
            OPTIONAL {{ ?param rdfs:label ?label . }} .
            OPTIONAL {{ ?param bigowl:hasName ?name. }} .
            OPTIONAL {{ ?param bigowl:hasRange ?range . }} .
            OPTIONAL {{ ?param bigowl:hasDefaultValue ?defaultValue . }}            
        }}
    """
    GET_INPUT_CLASSES = """
        SELECT DISTINCT ?in ?type ?name 
        WHERE {{
            <{component}> bigowl:specifiesInputClass ?in .
            ?in rdf:type ?type . 
            OPTIONAL {{ ?in rdfs:label ?name . }} .
            FILTER (?type != owl:NamedIndividual)
        }} 
    """
    GET_OUTPUT_CLASSES = """
        SELECT DISTINCT ?out ?type ?name
        WHERE {{
            <{component}> bigowl:specifiesOutputClass ?out .
            ?out rdf:type ?type . 
            OPTIONAL {{ ?out rdfs:label ?name . }} .
            FILTER (?type != owl:NamedIndividual)
        }} 
    """
    GET_COMPATIBLE_COMPONENTS = """
        SELECT DISTINCT ?component2 ?label ?classComponent2
        WHERE {{
            <{component}> bigowl:specifiesOutputClass ?data .
            ?data rdf:type ?type.
            ?classComponent2  rdfs:subClassOf* bigowl:Component .
            ?component2 rdf:type ?classComponent2 .
            OPTIONAL {{ ?component2 rdfs:label ?label . }} .
            ?component2 bigowl:specifiesInputClass ?classin .
            ?classin rdf:type ?generaltype .
            ?type  rdfs:subClassOf* ?generaltype .
            FILTER (?type!=owl:NamedIndividual)
        }}
    """
    GET_IMPLEMENTATION = """
        SELECT ?language ?module
        WHERE {{
            <{component}> bigowl:hasImplementation ?implementation.
            ?implementation bigowl:implementationLanguage ?language .
            ?implementation bigowl:module ?module .
        }}
    """
    GET_PARENTS = """
        SELECT ?parent
        WHERE {{ 
            <{individual}> rdf:type ?class . 
            FILTER (?class!=owl:NamedIndividual)
            ?class rdfs:subClassOf * ?parent
        }}
    """
    ARE_COMPONENTS_FROM_WORKFLOW_VALID = """
        SELECT ?task1 ?task2
        WHERE {{
            <{workflow}> rdf:type dmop:Workflow .
            <{workflow}> bigowl:hasTask ?task1 .
            <{workflow}> bigowl:hasTask ?task2 .
            ?task1 rdf:type bigowl:Task .
            ?task2 rdf:type bigowl:Task .
            ?task1 bigowl:connectedTo ?task2 .
            ?task1 bigowl:hasComponent ?comp1 .
            ?task2 bigowl:hasComponent ?comp2 .
            OPTIONAL {{ ?comp1 rdfs:label ?name1 . }}
            OPTIONAL {{ ?comp2 rdfs:label ?name2 . }}
            MINUS {{
                SELECT DISTINCT ?task1 ?task2
                WHERE {{
                    <{workflow}> rdf:type dmop:Workflow .
                    <{workflow}> bigowl:numTask ?num_task .
                    <{workflow}> bigowl:hasTask ?task1 .
                    <{workflow}> bigowl:hasTask ?task2 .
                    ?task1 rdf:type bigowl:Task .
                    ?task2 rdf:type bigowl:Task .
                    ?task1 bigowl:connectedTo ?task2 .
                    ?task1 bigowl:hasComponent ?comp1 .
                    ?task2 bigowl:hasComponent ?comp2 .
                    OPTIONAL {{ ?comp1 rdfs:label ?name1 . }}
                    OPTIONAL {{ ?comp2 rdfs:label ?name2 . }}
                    ?comp1 bigowl:specifiesOutputClass ?outputC .
                    ?comp2 bigowl:specifiesInputClass ?inputC .
                    ?task1 bigowl:specifiesOutputClass ?outputTask .
                    ?task2 bigowl:specifiesInputClass ?inputTask .
                    ?inputC rdf:type  ?class .
                    ?outputC rdf:type ?classOC .
                    ?outputTask rdf:type ?classOT .
                    ?inputTask rdf:type ?classIT .
                    ?classOC rdfs:subClassOf* ?class .
                    ?classOT rdfs:subClassOf* ?class .
                    ?classIT rdfs:subClassOf* ?class .
                    FILTER (?class!=owl:NamedIndividual)                               
                }}
                GROUP BY ?task1 ?task2 
            }}
        }}
    """


class BIGOWL:
    def __init__(self, db: RDFRepository):
        self.db = db

    @cached(ttl=3600)
    async def components(
        self,
        component_type: str = None,
        include_parameters: bool = False,
        include_connections: bool = False,
    ) -> dict:
        """
        Returns components on the database (including the number of entities found).
        """
        operators = []

        if not component_type:
            components = await self.db.query(query=BIGOWLQueries.GET_COMPONENTS, type="Component")
        else:
            components = await self.db.query(query=BIGOWLQueries.GET_COMPONENTS, type=component_type)

        for component in components:
            individual_uri = component["individual"]["value"]
            individual_name = get_name(individual_uri)

            # optional fields
            label = component.get("label", {}).get("value", individual_name)
            description = component.get("description", {}).get("value")

            # properties
            module_implementations = []
            implementations = await self.db.query(query=BIGOWLQueries.GET_IMPLEMENTATION, component=individual_uri)

            for imp in implementations:
                try:
                    module_implementations.append(
                        {
                            "language": imp["language"]["value"],
                            "module": imp["module"]["value"],
                        }
                    )
                except:
                    logger.exception(f"Could not parse implementation: {imp}")

            # optional: parameters
            parameters = []
            if include_parameters:
                try:
                    parameters = await self.parameters(individual_uri)
                except:
                    logger.exception("Could not parse parameters")

            # optional: ins/outs
            inputs, outputs = [], []
            if include_connections:
                try:
                    inputs = await self.inputs(individual_uri)
                    outputs = await self.outputs(individual_uri)
                except:
                    logger.exception("Could not parse connections")

            operators.append(
                {
                    "properties": {
                        # required
                        "name": individual_name,
                        "label": label,
                        "description": description,
                        "module": module_implementations,
                        "ninputs": len(inputs),
                        "noutputs": len(outputs),
                        # optional
                        "parameters": parameters,
                        "inputs": inputs,
                        "outputs": outputs,
                    },
                    "definition": {
                        "uri": individual_uri,
                        "type": component["type"]["value"],
                    },
                }
            )

        return {component_type: {"operators": operators, "total": len(operators)}}

    async def compatible(self, uri: str) -> dict:
        """
        Returns compatible components of a particular component (including the total number of entities).
        """
        operators = []
        compatible_components = await self.db.query(query=BIGOWLQueries.GET_COMPATIBLE_COMPONENTS, component=uri)

        logger.debug(compatible_components)

        for component in compatible_components:
            individual_uri = component["component2"]["value"]
            individual_name = get_name(individual_uri)

            # optional fields
            label = component.get("label", {}).get("value", individual_name)

            operators.append(
                {
                    "properties": {"name": individual_name, "label": label},
                    "definition": {
                        "uri": individual_uri,
                        "type": component["classComponent2"]["value"],
                    },
                }
            )

        return {"compatible_components": operators, "total": len(operators)}

    async def parameters(self, uri: str) -> list:
        """
        Returns the parameters of a particular component.
        """
        params = []
        parameters = await self.db.query(query=BIGOWLQueries.GET_PARAMETERS, component=uri)

        logger.debug(parameters)

        for p in parameters:
            params.append(
                {
                    "properties": {
                        "name": p.get("name", {}).get("value"),
                        "label": p.get("label", {}).get("value"),
                        "value": p.get("defaultValue", {}).get("value"),
                        "defaultValue": p.get("defaultValue", {}).get("value"),
                    },
                    "definition": {
                        "uri": p["param"]["value"],
                        "type": p["type"]["value"],
                    },
                }
            )

        return params

    async def inputs(self, uri: str) -> list:
        """
        Returns the input(s) of a particular component.
        """
        inns = []
        inputs = await self.db.query(query=BIGOWLQueries.GET_INPUT_CLASSES, component=uri)

        logger.debug(inputs)

        for inn in inputs:
            individual_uri = inn["in"]["value"]
            individual_name = get_name(individual_uri)

            inns.append(
                {
                    "properties": {"name": individual_name},
                    "definition": {
                        "uri": individual_uri,
                        "type": inn["type"]["value"],
                    },
                }
            )

        return inns

    async def outputs(self, uri: str) -> list:
        """
        Returns the output(s) of a particular component.
        """
        outs = []
        outputs = await self.db.query(query=BIGOWLQueries.GET_OUTPUT_CLASSES, component=uri)

        logger.debug(outputs)

        for out in outputs:
            individual_uri = out["out"]["value"]
            individual_name = get_name(individual_uri)

            outs.append(
                {
                    "properties": {"name": individual_name},
                    "definition": {
                        "uri": individual_uri,
                        "type": out["type"]["value"],
                    },
                }
            )

        return outs

    async def parents(self, uri: str) -> List[str]:
        """
        Returns the parent(s) of a connection.
        """
        parents = await self.db.query(query=BIGOWLQueries.GET_PARENTS, individual=uri)
        logger.debug(parents)

        return [parent["parent"]["value"] for parent in parents]

    async def check_invalid_components(self, uri: str = None) -> list:
        """
        Check for invalid connected components.
        """
        invalid = await self.db.query(query=BIGOWLQueries.ARE_COMPONENTS_FROM_WORKFLOW_VALID, workflow=uri)
        results = []

        for pair in invalid:
            individual_uri_one = pair["comp1"]["value"]
            individual_uri_two = pair["comp2"]["value"]

            results.append(
                {
                    0: {
                        "name": individual_uri_one["name"]["value"],
                    },
                    1: {
                        "name": individual_uri_two["name"]["value"],
                    },
                }
            )

        return results
