"""
utils.py - contains a variety of utility classes and functions useful
for processing filesystem actions
"""
import os

class Path:
	"""
	Takes in a path and separates it into its components
	"""
	def __init__(self, rel_path, root=None):
		self.raw_path = os.path.join(root, rel_path.strip('/')) if root else rel_path
		self.components = rel_path.strip('/').split('/')

	@staticmethod
	def join_paths(path1, path2):
		path1_raw = path1.get_path()
		path2_raw = path2.get_path().lstrip('/')
		new_components = path1.get_components() + path2.get_components()

		raw_path = os.path.join(path1_raw, path2_raw)
		new_path = Path(raw_path)
		new_path.components = new_components
		return new_path

	def is_root(self, root):
		return self.raw_path.strip('/') == root.strip('/')

	def get_action(self):
		return self.components[0]

	def get_query(self):
		if len(self.components) > 1:
			return self.components[1]
		return None

	def get_components(self):
		return self.components

	def get_path(self):
		return self.raw_path