from hunabku.HunabkuBase import HunabkuPluginBase, endpoint


class Hello(HunabkuPluginBase):
    def __init__(self, hunabku):
        super().__init__(hunabku)

    @endpoint('/hello', methods=['GET'])
    def hello(self):
        """
        @api {get} /hello/:id Simple Hello with ID
        @apiName Hello
        @apiGroup Hello

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
