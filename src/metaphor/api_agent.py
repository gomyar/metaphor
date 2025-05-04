
import os
import json

from langchain_core.tools import tool

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser
from langchain.agents import initialize_agent
from langchain.agents.agent_types import AgentType
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langgraph.prebuilt import create_react_agent

from typing_extensions import TypedDict, Annotated
from pydantic import BaseModel
from langfuse.callback import CallbackHandler


from metaphor.schema_serializer import serialize_schema

import logging

log = logging.getLogger()


class ApiAgent:
    PROMPT = '''
        You are an Agent for communicating with an API. You will take user requests to read and update resources in the API.
        Each resource in the API may be accessed by its canonical url or may be linked to other resources through fields.
        Resources contain fields which may be either simple types or complex types.
        Simple types may be one of int, str, float, boolean.
        Complex types may be one of the following:
          - collection - a collection of child resources
          - link - a link to another resource of a given spec
          - linkcollection - a collection of links to other resources of a given spec
          - calc - a read only calculated field

        Field names may only consist of lowercase letters and underscores. Where multiple words are given for a field, join them with underscores.
        Spec names may only consist of lowercase letters and underscores. Where multiple words are given for a spec, join them with underscores.

        Each path to a resource is a url which may start at the fields of the root resource. The available root collections are {{list(schema_structure['root']['fields'].keys())}}. If no path exists to the resource then return an error and stop.

        Paths follow the structure of the schema, i.e.:
          - if the schema has an employee spec, and the root has a "employees" collection, then new employees may be created at the path: "/employees"
          - if the schema has an employee spec, and an organization spec, and the root has an "organizations" collection, and organizations has an "employees" collection, if the current organization has an id of 1234, then new employees may be created at the path: "/organizations/1234/employees"

        Never make up values for fields which are not explicitly given. If the field value is not explicitly given, then it has a value of None. If a resource requires a value which is not given (as specified by "required" in the field definition itself), then return an error and stop.

        The API has the following schema:
        {schema_structure}

        {user_prompt}
    '''
    def __init__(self, api, schema):
        self.api = api
        self.schema = schema
        self.model = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0)
        self.graph = create_react_agent(self.model, [
            self.create_resource,
            self.update_resource,
            self.delete_resource,
            self.get_resource,
            self.find_resources,
        ])

    def prompt(self, user_prompt):
        schema_structure = json.dumps(serialize_schema(self.schema), indent=4)
        prompt = ApiAgent.PROMPT.format(
            schema_structure=schema_structure,
            user_prompt=user_prompt)
        langfuse_callback = CallbackHandler()
        return self.graph.invoke({
            "messages": [prompt]},
            config={"callbacks": [langfuse_callback], "recursion_limit": 5})

    def create_resource(self, path: str, fields: dict):
        """ Creates a new resource in the collection specified by the given path, using the field values.
            Args:
                path: full path to the resource
                fields: dictionary of any field values given
        """
        resource_id = self.api.post(path, fields)
        new_path = os.path.join(path, resource_id)
        resource = self.api.get(path)

        return {
            "messages": f"New resource created with id {resource_id}",
            "current_resource_path": new_path,
            "current_resource": resource,
        }

    def update_resource(self, path: str, fields: dict):
        """ Updates the fields in the resource with the given path
            Args:
                path: full path to the resource
                fields: dictionary of any field values given
        """
        self.api.patch(path, fields)
        resource = self.api.get(path)

        return {
            "messages": "resource updated",
            "current_resource_path": path,
            "current_resource": resource,
        }

    def delete_resource(self, path: str):
        """ Deletes the resource with the given path
            Args:
                path: full path to the resource
        """
        self.api.delete(path)
        path_parts = path.strip('/').split('/')
        new_path = "/" + "/".join(path_parts[:-1])
        resource = self.api.get(new_path)

        return {
            "messages": "resource deleted",
            "current_resource_path": new_path,
            "current_resource": resource,
        }

    def get_resource(self, path:str):
        """ Gets a single resource from the api with the given path
            Args:
                path: full path to the resource
        """
        resource = self.api.get(new_path)

        return {
            "messages": "got resource",
            "current_resource_path": path,
            "current_resource": resource,
        }

    def find_resources(self):
        """ Gets a single resource from the api with the given path.
            Each resource in the path may have a filter applied to it.
            Args:
                path: full path to the resource
        """
        resource = self.api.get(new_path)

        return {
            "messages": "got resource",
            "current_resource_path": path,
            "current_resource": None,
        }
