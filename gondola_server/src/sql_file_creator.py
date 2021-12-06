from jinja2 import Environment, FileSystemLoader


class SqlTemplateEnvironment:
    """

    """

    def __init__(self, searchpath):

        self.file_loader = FileSystemLoader(searchpath=searchpath)
        self.env = Environment(loader=self.file_loader)

    def get_template(self, sql_file):
        template = self.env.get_template(sql_file)
        return template


class SqlFileCreator:
    """

    """

    def __init__(self):
        self._templates = {}

    def register_template(self, key, template):
        self._templates[key] = template

    def create_sql_file(self, parameters, key):

        """

        :param migration:
        :param template:
        :return:
        """
        template = self._templates.get(key)
        if not template:
            raise ValueError(key)
        return template.render(parameters=parameters)
