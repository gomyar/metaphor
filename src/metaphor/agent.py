
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
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

from typing_extensions import TypedDict, Annotated
from pydantic import BaseModel
from langfuse.callback import CallbackHandler


from metaphor.schema_serializer import serialize_schema

import logging

log = logging.getLogger()


class SchemaEditorAgent:
    PROMPT = '''
        You are an api schema manager agent. You are responsible for making changes to a the schema for an API.  You will only use the available tools to perform actions. You handle creating, editing, and deleting of the specs in the schema and the fields in each spec. Each field may be either a simple or complex type. Simple types are str, int, float, bool. Complex types may be collections of specs, collections of links to other specs, or single links to another spec. Calc types are read only calculated fields which may return any type.

        The current Schema has the following structure:
        {schema_structure}

        {user_prompt}
    '''
    CALC_PROMPT = """
        You are a calculation creator agent. You are responsible for translating a user request into a usable calculation against a given schema. The calculation takes the form of a DSL with a more or less mathematical structure. The calculation will be based on the specs and fields in the schema. Terms in the calculation evaluate to a spec or field or simple type. The evaluation begins at the fields in the root spec or relative to the spec in question using the 'self' keyword, which always refers to an instance of that spec. Filtering on collections may be accomplished using terms within square brackets.

        - If the schema has a spec called student, with a field called age, and the user says "add a field to student which is the age of the student plus 1" the calc is: "self.age + 1". This is because self refers to any instance of the student spec.
        - If the schema has a root spec with a field called "students" which is a collection of students, and the user says "add a field to another spec which is all the students over the age of 18" the calc is: "students[age>18]". This is because students is a field in the root spec and the square brackets filter on the students collection.

        All specs and fields referenced must exist in the schema.

        The current Schema has the following structure:
        {schema_structure}

        The user prompt is:
        {user_prompt}
    """
    def __init__(self, schema):
        self.schema = schema
        self.model = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0)
        self.tools = {
            "create_new_spec": self.create_new_spec,
            "create_new_basic_field": self.create_new_basic_field,
            "create_new_link_field": self.create_new_link_field,
            "create_new_collection_field": self.create_new_collection_field,
            "create_new_link_collection_field": self.create_new_link_collection_field,
            "delete_field": self.delete_field,
            "delete_spec": self.delete_spec,
        }
        self.tooled_model = self.model.bind_tools(self.tools.values())
        self.calc_tools = {
            "create_new_calc": self.create_new_calc,
        }
        self.calc_model = self.model.bind_tools(self.calc_tools.values())

        class State(TypedDict):
            messages: Annotated[list, add_messages]

        graph_builder = StateGraph(State)

        graph_builder.add_conditional_edges(START, self.choose_path_node)
        graph_builder.add_node("calc", self.call_calc_tool_node)
        graph_builder.add_node("tools", self.call_tool_node)

        graph_builder.add_edge("tools", END)
        graph_builder.add_edge("calc", END)

        self.graph = graph_builder.compile()

    def call_tool_node(self, state):
        response = self.tooled_model.invoke(state["messages"])
        tool_responses = []
        for tool_call in response.tool_calls:
            log.debug("Calling tool: %s", tool_call)
            tool_responses.append(
                ToolMessage(self.tools[tool_call['name']](**(tool_call['args'])),
                            tool_call_id=tool_call["id"])
            )
        return {"messages": tool_responses}

    def call_calc_tool_node(self, state):
        log.debug("Adding calc")

        last_message = self._last_human_message(state).content
        schema_structure = json.dumps(serialize_schema(self.schema), indent=4)
        prompt = SchemaEditorAgent.CALC_PROMPT.format(
            schema_structure=schema_structure,
            user_prompt=last_message)
        langfuse_callback = CallbackHandler()
        response = self.calc_model.invoke(prompt,
            config={"callbacks": [langfuse_callback]})

        tool_responses = []
        for tool_call in response.tool_calls:
            log.debug("Calling tool: %s", tool_call)
            tool_responses.append(
                ToolMessage(self.calc_tools[tool_call['name']](**(tool_call['args'])),
                            tool_call_id=tool_call["id"])
            )
        return {"messages": tool_responses}

    def _last_human_message(self, state):
        for message in reversed(state['messages']):
            if isinstance(message, HumanMessage):
                return message
        return None

    def choose_path_node(self, state):
        last_message = self._last_human_message(state).content
        prompt = f"""You are a router that determines which task to perform based on a user prompt. You must choose exactly one of the following task identifiers:

        - "calc": Use if the user is asking to create a calculated field, that is a field which is the product, sum, or other combination of other fields.
        - "basic": Use if the user is asking to alter the schema, that is create / update / delete a spec or field.
        - "none": Use if the request does not match schema altering requests.

        Guidelines:
        - Consider the intent of the user, not just the keywords.
        - Respond ONLY with one of: "calc", "basic", or "none".
        - DO NOT include explanations or full sentences. Just return the identifier.

        {last_message}
        """
        response = self.model.invoke(prompt)
        if response.content == 'calc':
            return 'calc'
        else:
            return 'tools'

    def tools_node(self, state):
        last_message = state['messages'][-1]

    def prompt(self, user_prompt):
        schema_structure = json.dumps(serialize_schema(self.schema), indent=4)
        prompt = SchemaEditorAgent.PROMPT.format(
            schema_structure=schema_structure,
            user_prompt=user_prompt)
        langfuse_callback = CallbackHandler()
        return self.graph.invoke({
            "messages": [prompt]},
            config={"callbacks": [langfuse_callback]})

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

    def create_new_calc(self, spec_name:str, field_name: str, calc_str: str):
        """ Creates a new calc field on the given spec
            Args:
                spec_name: Name of the spec the field is in
                field_name: The Name of the field
                calc_str: The calc string to add
        """
        self.schema.create_field(spec_name, field_name, "calc", calc_str=calc_str)
        return "Calc created"
