"""
tag_ops.py - contains classes and functions relevant to
parsing and processing tag queries
"""

import ast
import os

class TagQuery(object):
	"""
	Base class for performing tag query operations.
	Should be overridden by implementation classes.
	"""
	def __init__(self, tag_folder_path=None):
		self.tag_folder_path = tag_folder_path

	def eval(self, tag_folder_path=None):
		"""
		Must be overridden. Should return the set of tags
		which correspond to the operation.
		"""
		if tag_folder_path:
			self.tag_folder_path = tag_folder_path
		if not self.tag_folder_path:
			raise ValueError("Missing value for folder containing tags")
		return set()

class BinTagQuery(TagQuery):

	UNION = "+" # In either
	INTERSECT = "&" # In both
	DIFF = "-" # From left but not right
	SYMDIFF = "^" # From left or right but not both 
	VALID_OPS = [UNION, INTERSECT, DIFF, SYMDIFF]

	def __init__(self, left, right, op, tag_folder_path=None):
		if op not in self.VALID_OPS:
			raise ValueError("Operator must be one of '" + " ".join(self.VALID_OPS) + "'.")

		super().__init__(tag_folder_path)
		self.left = left
		self.right = right
		self.op = op

	def eval(self, tag_folder_path=None):
		TagQuery.eval(self, tag_folder_path)

		left_set = self.left.eval(tag_folder_path)
		right_set = self.right.eval(tag_folder_path)

		# Perform join operation
		if self.op == self.UNION:
			return left_set | right_set
		elif self.op == self.INTERSECT:
			return left_set & right_set
		elif self.op == self.DIFF:
			return left_set - right_set
		elif self.op == self.SYMDIFF:
			return left_set ^ right_set
		
		raise NotImplementedError("Missing implementation for operator: " + self.op)

class UnaryTagQuery(TagQuery):
	def __init__(self, tag, tag_folder_path=None):
		super().__init__(tag_folder_path)
		self.tag = tag

	def eval(self, tag_folder_path=None):
		TagQuery.eval(self, tag_folder_path)

		# Search tag directory for the tag, and return the corresponding inodes
		tag_dir = os.path.join(self.tag_folder_path, self.tag)
		if not os.path.isdir(tag_dir):
			raise NotADirectoryError("No directory for tag: " + self.tag)

		inodes = [int(inode.strip("/")) for inode in os.listdir(tag_dir)]
		return set(inodes)

class TagQueryParser():
	"""
	Parses a string into a TagQuery, which can be
	evaluated to get the set of tags associated with
	the query.
	"""

	def __init__(self, expr_str):
		self.expr = ast.parse(expr_str).body[0]

		if type(self.expr) != ast.Expr:
			raise ValueError("Failed to parse tag query string")

	def parseUnaryOp(self, unop):
		if type(unop) != ast.Name:
			raise ValueError("Argument must be an expression of type ast.Name")
		return UnaryTagQuery(unop.id)		

	def parseBinOp(self, binop):
		if type(binop) != ast.BinOp:
			raise ValueError("Argument must be an expression of type ast.BinOp")

		# Parse each side of the BinOp
		left, right = self.parseValue(binop.left), self.parseValue(binop.right)

		# Process the operator
		op = None
		optype = type(binop.op)

		if optype == ast.Add:
			op = BinTagQuery.UNION
		elif optype == ast.BitAnd:
			op = BinTagQuery.INTERSECT
		elif optype == ast.Sub:
			op = BinTagQuery.DIFF
		elif optype == ast.BitXor:
			op = BinTagQuery.SYMDIFF

		if not op:
			raise ValueError("Bad binary operator")

		return BinTagQuery(left, right, op)

	def parseValue(self, value):
		if type(value) == ast.Name:
			return self.parseUnaryOp(value)
		elif type(value) == ast.BinOp:
			return self.parseBinOp(value)

	def parse(self):
		return self.parseValue(self.expr.value)

def get_query_inodes(query, tag_folder):
    return list(TagQueryParser(query).parse().eval(tag_folder))

