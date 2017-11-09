
from calclang import lang

import ply.lex as lex
lexer = lex.lex(module=lang)


import ply.yacc as yacc
_parser = yacc.yacc(module=lang)


def parse(schema, expr):
    return _parser.parse(expr)
