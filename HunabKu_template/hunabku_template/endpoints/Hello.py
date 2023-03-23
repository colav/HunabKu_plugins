from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param


class Hello(HunabkuPluginBase):
    config = Config()
    config += Param(myvar="myvalue", doc="this is an example var")
    config.subcategory += Param(port=8080).doc("this is other way to set doc")
    config.subcategory += Param(host="localhost")  # may you want a var without doc

    def __init__(self, hunabku):
        super().__init__(hunabku)

    @endpoint('/hello', methods=['GET'])
    def hello(self):
        """
        @api {get} /hello/:id Hello
        @apiName Hello
        @apiGroup Template


        """
        if self.valid_apikey():
            response = self.app.response_class(
                response=self.json.dumps({'hello': 'world'}),
                status=200,
                mimetype='application/json'
            )
            return response
        else:
            return self.apikey_error()
