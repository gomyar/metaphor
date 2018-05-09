
# parsetab.py
# This file is automatically generated. Do not edit.
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'leftPLUSMINUSleftTIMESDIVIDErightUMINUSNAME NUMBER PLUS MINUS TIMES DIVIDE EQUALS LPAREN RPARENstatement : expressionexpression : NAME LPAREN expression RPARENexpression : expression PLUS expression\n                  | expression MINUS expression\n                  | expression TIMES expression\n                  | expression DIVIDE expressionexpression : MINUS expression %prec UMINUSexpression : LPAREN expression RPARENexpression : NUMBERexpression : NAME'
    
_lr_action_items = {'RPAREN':([1,2,8,13,14,15,16,17,18,19,20,],[-10,-9,15,-7,20,-8,-3,-6,-4,-5,-2,]),'DIVIDE':([1,2,5,8,13,14,15,16,17,18,19,20,],[-10,-9,10,10,-7,10,-8,10,-6,10,-5,-2,]),'NUMBER':([0,4,6,7,9,10,11,12,],[2,2,2,2,2,2,2,2,]),'TIMES':([1,2,5,8,13,14,15,16,17,18,19,20,],[-10,-9,12,12,-7,12,-8,12,-6,12,-5,-2,]),'PLUS':([1,2,5,8,13,14,15,16,17,18,19,20,],[-10,-9,9,9,-7,9,-8,-3,-6,-4,-5,-2,]),'LPAREN':([0,1,4,6,7,9,10,11,12,],[4,7,4,4,4,4,4,4,4,]),'$end':([1,2,3,5,13,15,16,17,18,19,20,],[-10,-9,0,-1,-7,-8,-3,-6,-4,-5,-2,]),'MINUS':([0,1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,],[6,-10,-9,6,11,6,6,11,6,6,6,6,-7,11,-8,-3,-6,-4,-5,-2,]),'NAME':([0,4,6,7,9,10,11,12,],[1,1,1,1,1,1,1,1,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'expression':([0,4,6,7,9,10,11,12,],[5,8,13,14,16,17,18,19,]),'statement':([0,],[3,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> statement","S'",1,None,None,None),
  ('statement -> expression','statement',1,'p_statement_expr','lang.py',63),
  ('expression -> NAME LPAREN expression RPAREN','expression',4,'p_expression_function','lang.py',67),
  ('expression -> expression PLUS expression','expression',3,'p_expression_binop','lang.py',71),
  ('expression -> expression MINUS expression','expression',3,'p_expression_binop','lang.py',72),
  ('expression -> expression TIMES expression','expression',3,'p_expression_binop','lang.py',73),
  ('expression -> expression DIVIDE expression','expression',3,'p_expression_binop','lang.py',74),
  ('expression -> MINUS expression','expression',2,'p_expression_uminus','lang.py',81),
  ('expression -> LPAREN expression RPAREN','expression',3,'p_expression_group','lang.py',89),
  ('expression -> NUMBER','expression',1,'p_expression_number','lang.py',93),
  ('expression -> NAME','expression',1,'p_expression_name','lang.py',97),
]
