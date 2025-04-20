
import os
import json

from langchain_core.tools import tool

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser
from langchain.agents import initialize_agent
from langchain.agents.agent_types import AgentType
from langchain.tools import StructuredTool
from langgraph.prebuilt import create_react_agent
from pydantic import BaseModel

from metaphor.schema_serializer import serialize_schema


class SchemaEditorAgent:
    PROMPT = '''
        You are an api schema manager agent. You are responsible for making changes to a the schema for an API.  You will only use the available tools to perform actions. You handle creating, editing, and deleting of the specs in the schema and the fields in each spec. Each field may be either a simple or complex type. Simple types are str, int, float, bool. Complex types may be collections of specs, collections of links to other specs, or single links to another spec. Calc types are read only calculated fields which may return any type.

        The current Schema has the following structure:
        {schema_structure}

    '''
    def __init__(self, schema):
        self.schema = schema
        model = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0)

        self.graph = create_react_agent(model, tools=[
            self.create_new_spec,
            self.create_new_basic_field,
            self.create_new_link_field,
            self.create_new_collection_field,
            self.create_new_link_collection_field,
            self.delete_field,
            self.delete_spec,
        ])

    def prompt(self, text):
        schema_structure = json.dumps(serialize_schema(self.schema), indent=4)
        return self.graph.invoke({"messages": [text]})

    def create_new_spec(self, spec_name:str) -> str:
        """ Creates a new spec with the given name and optional description
            Args:
                spec_name: Unique name for the new spec
        """
        self.schema.create_spec(spec_name)
        return "Spec created"

    def create_new_basic_field(self, spec_name:str, field_name:str, field_type:str) -> str:
        """ Creates a new basic field on the given spec
            Args:
                spec_name: The name of the existing spec in which to add the field
                field_name: The name of the new field
                field_type: The type of the new field, one of ('str', 'int', 'float', 'bool')
        """
        self.schema.create_field(spec_name, field_name, field_type)
        return "Field created"

    def create_new_link_field(self, spec_name:str, field_name:str, target_spec_name:str) -> str:
        """ Creates a field on the given spec which is a single link to another spec
            Args:
                spec_name: The name of the existing spec in which to add the field
                field_name: The name of the new field
                target_spec_name: The name of the spec to link to
        """
        self.schema.create_field(spec_name, field_name, "link", field_target=target_spec_name)
        return "Link created"

    def create_new_collection_field(self, spec_name:str, field_name:str, child_spec_name:str) -> str:
        """ Creates a field on the given spec which is a collection of child specs
            Args:
                spec_name: The name of the existing spec in which to add the field
                field_name: The name of the new field
                child_spec_name: The name of the existing child spec
        """
        self.schema.create_field(spec_name, field_name, "collection", field_target=child_spec_name)
        return "Collection created"

    def create_new_link_collection_field(self, spec_name:str, field_name:str, target_spec_name:str) -> str:
        """ Creates a field on the given spec which is a collection of links to existing specs
            Args:
                spec_name: The name of the existing spec to add the field
                field_name: The name of the new field
                target_spec_name: The name of the existing target spec
        """
        self.schema.create_field(spec_name, field_name, "linkcollection", field_target=target_spec_name)
        return "Link Collection created"

    def create_new_calc_field(self, params):
        pass

    def delete_field(self, spec_name:str, field_name:str) -> str:
        """ Deletes a field from the given spec
            Args:
                spec_name: Name of the spec the field is in
                field_name: The Name of the field
        """
        self.schema.delete_field(spec_name, field_name)
        return "Field deleted"

    def delete_spec(self, spec_name:str) -> str:
        """ Deletes the given spec
            Args:
                spec_name: Name of the spec
        """
        self.schema.delete_spec(spec_name)
        return "Spec deleted"
