from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from flask import redirect


class Index(HunabkuPluginBase):
    config = Config()
    config += Param(url="/apidoc/index.html", doc="url to redirect index")

    def __init__(self, hunabku):
        super().__init__(hunabku)

    @endpoint('/', methods=['GET'])
    def index(self):
        # Not apidoc required, this plugin doesn´t provide an API
        return redirect(self.config.url)
