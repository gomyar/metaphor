
from metaphor.operators import Calc, ConstRef, AddOp, SubtractOp, MultiplyOp
from metaphor.operators import DividebyOp
from metaphor.operators import Func
from metaphor.operators import ResourceRef
from metaphor.operators import FilterOneFunc
from metaphor.operators import FilterMaxFunc
from metaphor.operators import FilterMinFunc
from metaphor.operators import ExpCondition
from metaphor.operators import ResourceFilter

tokens = (
    'NAME','NUMBER','STRING',
    'PLUS','MINUS','TIMES','DIVIDE',
    'EQUALS','GT','LT','GTE','LTE','LIKE','AND','OR',
    'LPAREN','RPAREN','LSQPAREN','RSQPAREN','COMMA',
    )

literals = ['[', ']']
# Tokens

t_PLUS    = r'\+'
t_MINUS   = r'-'
t_TIMES   = r'\*'
t_DIVIDE  = r'/'
t_EQUALS  = r'='
t_GT      = r'\>'
t_LT      = r'\<'
t_GTE     = r'\>='
t_LTE     = r'\<='
t_LIKE    = r'\~='
t_AND     = r'&'
t_OR      = r'\|'
t_LPAREN  = r'\('
t_RPAREN  = r'\)'
t_LSQPAREN  = r'\['
t_RSQPAREN  = r'\]'
t_NAME    = r'[a-zA-Z_][a-zA-Z0-9_\.]*'
t_COMMA   = r','


def t_NUMBER(t):
    r'\d+[\.\d+]?'
    try:
        t.value = float(t.value)
    except ValueError:
        print("Integer value too large %d", t.value)
        t.value = 0
    return t


def t_STRING(t):
    r'("[a-zA-Z]*")|(\'[a-zA-Z]*\')'
    t.value = str(t.value[1:-1])
    return t


# Ignored characters
t_ignore = " \t"

def t_newline(t):
    r'\n+'
    t.lexer.lineno += t.value.count("\n")

def t_error(t):
    raise Exception("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)

# Parsing rules

precedence = (
    ('left','PLUS','MINUS'),
    ('left','TIMES','DIVIDE'),
    ('right','UMINUS'),
    )

# dictionary of names
names = {
    'filter_one': FilterOneFunc,
    'filter_max': FilterMaxFunc,
    'filter_min': FilterMinFunc,
}

#def p_statement_assign(t):
#    'statement : NAME EQUALS expression'
#    names[t[1]] = t[3]

def p_statement_expr(t):
    '''statement : expression
                 | expression LSQPAREN expression RSQPAREN'''
    if len(t) == 2:
        t[0] = Calc(t[1])
    elif len(t) == 5 and type(t[1]) == ResourceRef:
        t[0] = ResourceFilter(t[1], t[3])
    elif len(t) == 5:
        raise Exception("Cannot filter %s" % (t[1],))
    else:
        raise Exception("Invalid expression %s" % (str(tt) for tt in t))

# Funny idiosyncrasy of ply - it doesn't seem to do two reduces in a row,
# so if we just use 'expression' here it will skip over NUMBERs and STRINGs
# because they have to reduce to 'expression' first (I think)
def p_expression_condition_equals(t):
    '''expression : NAME EQUALS NUMBER
                  | NAME EQUALS STRING
                  | NAME EQUALS expression
                  | NAME GT NUMBER
                  | NAME GT STRING
                  | NAME GT expression
                  | NAME LT NUMBER
                  | NAME LT STRING
                  | NAME LT expression
                  | NAME GTE NUMBER
                  | NAME GTE STRING
                  | NAME GTE expression
                  | NAME LTE NUMBER
                  | NAME LTE STRING
                  | NAME LTE expression
                  | NAME LIKE STRING
                  | NAME LIKE expression
                  | expression AND expression
                  | expression OR expression
    '''
    value = t[3]
    if isinstance(value, basestring) or type(value) in [float, int]:
        t[0] = ExpCondition(t[1], t[2], ConstRef(t[3]))
    else:
        t[0] = ExpCondition(t[1], t[2], t[3])


def p_expression_function(t):
    '''expression : NAME LPAREN expression RPAREN
                  | NAME LPAREN expression COMMA expression RPAREN
                  | NAME LPAREN expression COMMA expression COMMA expression RPAREN
    '''
    name = t[1]

    if name in names:
        t[0] = names[name](*(t[3::2]))
    else:
        t[0] = Func(t[1], t[3])

def p_expression_binop(t):
    '''expression : expression PLUS expression
                  | expression MINUS expression
                  | expression TIMES expression
                  | expression DIVIDE expression'''
    if t[2] == '+'  : t[0] = AddOp(t[1], t[3])
    elif t[2] == '-': t[0] = SubtractOp(t[1], t[3])
    elif t[2] == '*': t[0] = MultiplyOp(t[1], t[3])
    elif t[2] == '/': t[0] = DividebyOp(t[1], t[3])

def p_expression_uminus(t):
    'expression : MINUS expression %prec UMINUS'
    t[0] = ConstRef(-t[2].value)

def p_expression_group(t):
    'expression : LPAREN expression RPAREN'
    t[0] = t[2]

def p_expression_number(t):
    'expression : NUMBER'
    t[0] = ConstRef(t[1])

def p_expression_string(t):
    'expression : STRING'
    t[0] = ConstRef(t[1])

def p_expression_name(t):
    'expression : NAME'
    try:
        #        t[0] = names[t[1]]
        t[0] = ResourceRef(t[1])
    except LookupError:
        print("Undefined name '%s'" % t[1])
        t[0] = 0

def p_error(t):
    raise Exception("Syntax Error at line %s col %s '%s'" % (t.lineno, t.lexpos, t.value))
