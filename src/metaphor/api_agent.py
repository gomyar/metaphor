
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
    def __init__(self, api, schema, user=None):
        self.api = api
        self.schema = schema
        self.user = user
        self.model = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0)
        self.graph = create_react_agent(self.model, [
            self.create_resource,
            self.update_resource,
            self.delete_resource,
            self.get_resource,
            self.find_resources,
            self.link_resource,
        ])

    def prompt(self, user_prompt):
        schema_structure = json.dumps(serialize_schema(self.schema), indent=4)
        prompt = ApiAgent.PROMPT.format(
            schema_structure=schema_structure,
            user_prompt=user_prompt)
        langfuse_callback = CallbackHandler()
        return self.graph.invoke({
            "messages": [prompt]},
            config={"callbacks": [langfuse_callback], "recursion_limit": 10})

    def create_resource(self, path: str, fields: dict) -> dict:
        """ Creates a new resource in the collection specified by the given path, using the field values.
            Args:
                path: full path to the resource
                fields: dictionary of any field values given
        """
        resource_id = self.api.post(path, fields, user=self.user)
        new_path = os.path.join(path, resource_id)
        resource = self.api.get(path, user=self.user)

        return resource

    def update_resource(self, path: str, fields: dict) -> str:
        """ Updates the fields in the resource with the given path
            Args:
                path: full path to the resource
                fields: dictionary of any field values given
        """
        self.api.patch(path, fields, user=self.user)
        resource = self.api.get(path, user=self.user)

        return "resource updated"

    def delete_resource(self, path: str) -> str:
        """ Deletes the resource with the given path
            Args:
                path: full path to the resource
        """
        self.api.delete(path, user=self.user)
        path_parts = path.strip('/').split('/')
        new_path = "/" + "/".join(path_parts[:-1])
        resource = self.api.get(new_path, user=self.user)

        return "resource deleted"

    def get_resource(self, path:str) -> dict:
        """ Gets a single resource from the api with the given path
            Args:
                path: full path to the resource
        """
        resource = self.api.get(path, user=self.user)
        return resource

    def find_resources(self, path: str, query: dict) -> list[dict]:
        """ Gets a filtered list of resources from the given url. Filtered based on the fields in the query dict.
            Args:
                path: full path to the resource
                query: dict of fields to filter for
        """
        filter_str = ",".join(["%s~%s" % (f, json.dumps(v)) for (f, v) in query.items()])
        filter_str = f"[{filter_str}]" if filter_str else ""
        resource = self.api.get(path + filter_str, user=self.user)
        return resource['results']

    def link_resource(self, from_resource_path: str, field_name: str, to_resource_path: str) -> str:
        """ Link a resource to another using the given field name
            Args:
                from_resource_path: path of the resource from which to link
                field_name: name of the link field to set
                to_resource_path: target resource path
        """
        from_resource = self.api.get(from_resource_path, user=self.user)
        to_resource = self.api.get(to_resource_path, user=self.user)

        self.api.patch(from_resource_path, {field_name: to_resource['id']}, user=self.user)

        return "resource linked"
