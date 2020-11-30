import rdflib
from rdflib import RDF

from titan.models.workflow import WorkflowRequest, parse_to_list, parse_to_rdf

_workflow = {
    "operators": {
        "op_0": {
            "properties": {
                "name": "DemoTaskOne",
                "module": "DemoTaskOneModule",
                "inputs": {},
                "outputs": {
                    "out_0": {
                        "properties": {"name": "DemoTaskOneOutput"},
                        "definition": {"uri": "https://www.w3.org/#Data0"},
                    }
                },
            },
            "parameters": {
                "param_0": {
                    "properties": {"name": "param0", "value": 0},
                    "definition": {"uri": "https://www.w3.org/#Parameter0"},
                },
                "param_1": {
                    "properties": {"name": "param1", "value": "one"},
                    "definition": {"uri": "https://www.w3.org/#Parameter1"},
                },
            },
            "definition": {"uri": "https://www.w3.org/#Component0"},
        },
        "op_1": {
            "properties": {
                "name": "DemoTaskTwo",
                "module": "DemoTaskTwoModule",
                "inputs": {
                    "in_0": {
                        "properties": {"name": "DemoTaskTwoInput"},
                        "definition": {"uri": "https://www.w3.org/#Data0"},
                    }
                },
                "outputs": {},
            },
            "parameters": {},
            "definition": {"uri": "https://www.w3.org/#Component1"},
        },
    },
    "links": {"0": {"fromOperator": "op_0", "fromConnector": "out_0", "toOperator": "op_1", "toConnector": "in_0"}},
    "metadata": {"label": "test_parse_workflow"},
}


def test_valid_structure():
    WorkflowRequest(**_workflow)


def test_parsed_to_json():
    expected = [
        {"name": "DemoTaskOne", "module": "DemoTaskOneModule", "params": {"param0": 0, "param1": "one"}, "inputs": {}},
        {
            "name": "DemoTaskTwo",
            "module": "DemoTaskTwoModule",
            "params": {},
            "inputs": {"DemoTaskTwoInput": "DemoTaskOne.DemoTaskOneOutput"},
        },
    ]
    assert parse_to_list(_workflow) == expected


def test_parsed_to_rdf():
    result = parse_to_rdf(_workflow, workflow_id="test-1-1")
    # print(result.serialize(format="nt"))

    assert (
        rdflib.URIRef("http://www.khaos.uma.es/khaosteam/sensacion/titan-workflow#Workflowtest-1-1"),
        RDF.type,
        rdflib.URIRef("http://www.e-lico.eu/ontologies/dmo/DMOP/DMOP.owl#Workflow"),
    ) in result

    assert (
        rdflib.term.URIRef("http://www.khaos.uma.es/khaosteam/sensacion/titan-workflow#Workflowtest-1-1"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#numTask"),
        rdflib.term.Literal("2", datatype=rdflib.term.URIRef("http://www.w3.org/2001/XMLSchema#integer")),
    ) in result

    assert (
        rdflib.URIRef("http://www.khaos.uma.es/khaosteam/sensacion/titan-workflow#Workflowtest-1-1"),
        rdflib.URIRef("http://www.khaos.uma.es/perception/bigowl#hasTask"),
        rdflib.URIRef("https://www.w3.org/#Component0-test-1-1-op_0"),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component0-test-1-1-op_0"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#hasName"),
        rdflib.term.Literal("DemoTaskOne"),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component0-test-1-1-op_0"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#numberOfInputs"),
        rdflib.term.Literal("0", datatype=rdflib.term.URIRef("http://www.w3.org/2001/XMLSchema#integer")),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component0-test-1-1-op_0"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#numberOfOutputs"),
        rdflib.term.Literal("1", datatype=rdflib.term.URIRef("http://www.w3.org/2001/XMLSchema#integer")),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component0-test-1-1-op_0"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#specifiesOutputClass"),
        rdflib.term.URIRef("https://www.w3.org/#Data0"),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component0-test-1-1-op_0"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#hasComponent"),
        rdflib.term.URIRef("https://www.w3.org/#Component0"),
    ) in result

    assert (
        rdflib.URIRef("http://www.khaos.uma.es/khaosteam/sensacion/titan-workflow#Workflowtest-1-1"),
        rdflib.URIRef("http://www.khaos.uma.es/perception/bigowl#hasTask"),
        rdflib.URIRef("https://www.w3.org/#Component1-test-1-1-op_1"),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component1-test-1-1-op_1"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#hasName"),
        rdflib.term.Literal("DemoTaskTwo"),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component1-test-1-1-op_1"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#numberOfInputs"),
        rdflib.term.Literal("1", datatype=rdflib.term.URIRef("http://www.w3.org/2001/XMLSchema#integer")),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component1-test-1-1-op_1"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#specifiesInputClass"),
        rdflib.term.URIRef("https://www.w3.org/#Data0"),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component1-test-1-1-op_1"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#numberOfOutputs"),
        rdflib.term.Literal("0", datatype=rdflib.term.URIRef("http://www.w3.org/2001/XMLSchema#integer")),
    ) in result

    assert (
        rdflib.term.URIRef("https://www.w3.org/#Component1-test-1-1-op_1"),
        rdflib.term.URIRef("http://www.khaos.uma.es/perception/bigowl#hasComponent"),
        rdflib.term.URIRef("https://www.w3.org/#Component1"),
    ) in result
