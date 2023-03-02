from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param

class Hello(HunabkuPluginBase):
    config = Config()
    config += Param(myvar="myvalue", doc="this is an example var")
    config.subcategory += Param(port=8080).doc("this is other way to set doc")
    config.subcategory += Param(host="localhost") #may you want a var without doc
    
    def __init__(self, hunabku):
        super().__init__(hunabku)


    @endpoint('/hello', methods=['GET'])
    def hello(self):
        """
        @api {get} /hello/:id Hello
        @apiName Hello
        @apiGroup Template

        @apiParam {Number} id Users unique ID.

        @apiSuccess {String} firstname Firstname of the User.
        @apiSuccess {String} lastname  Lastname of the User.
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

    @endpoint('/config', methods=['GET'])
    def config_end(self):
        """
        @api {get} /config/ Config
        @apiDescription returns the plugin config, this endpoint doesn´t require apikey
        @apiName Config
        @apiGroup Template

        @apiSuccess {String} firstname Firstname of the User.
        @apiSuccess {String} lastname  Lastname of the User.
        """
        response = self.app.response_class(
            response=self.json.dumps(self.config.dict()),
            status=200,
            mimetype='application/json'
        )
        return response
