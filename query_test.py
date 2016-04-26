import tag_ops
import os

def printQuery(query, path):
	parser = tag_ops.TagQueryParser(query)
	operator = parser.parse()

	print("Query:" + query)
	print("Result:" + str(operator.eval(path)))

path = os.path.join(os.getcwd(), "tags")

# Test each operator
query = "awesome_tag+cool_tag"
printQuery(query, path)

query = "awesome_tag&cool_tag"
printQuery(query, path)

query = "awesome_tag-cool_tag"
printQuery(query, path)

query = "awesome_tag^cool_tag"
printQuery(query, path)