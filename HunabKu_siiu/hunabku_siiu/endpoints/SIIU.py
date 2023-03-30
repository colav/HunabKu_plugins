from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from pymongo import MongoClient


class SIIU(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config += Param(db_name="siiu",
                    doc="MongoDB name for SIIU")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)

    @endpoint('/siiu/project', methods=['GET'])
    def siiu_project(self):
        """
        @api {get} /siiu/project Project
        @apiName Project
        @apiGroup SIIU
        @apiDescription Allows to perform queries for projects,
                        you can search by project id or by keyword.
                        The search by keyword perform a search in teh fields of text
                        NOMBRE_CORTO, NOMBRE_COMPLETO, PALABRAS_CLAVES, descriptive_text.TEXTO_INGRESADO
                        lots of text where indexed for this search.

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} search  keyword for text search.
        @apiParam {String} CODIGO  project id.

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the products for the user
            curl -i http://apis.colav.co/siiu/project?apikey=XXXX&search=keyword
            # An specific product
            curl -i http://apis.colav.co/siiu/project?apikey=XXXX&CODIGO=2013-86

        """
        if self.valid_apikey():
            keyword = self.request.args.get('search')
            if keyword:
                data = list(self.dbclient[self.config.db_name]["project"].find({
                            "$text": {
                                "$search": keyword,
                                "$caseSensitive": False
                            }
                            }, {'_id': 0}))
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                return response
            data = {
                "error": "Bad Request", "message": "invalid parameters, please select the right combination of parameters."}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response

        else:
            return self.apikey_error()

    @endpoint('/siiu/info', methods=['GET'])
    def config_end(self):
        """
        @api {get} /siiu/info Info
        @apiName Info
        @apiGroup SIIU
        @apiDescription Allows to get information of the projects such as ids (CODIGO)

        @apiParam {String} apikey  Credential for authentication

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the products for the user
            curl -i http://apis.colav.co/siiu/info?apikey=XXXX
        """
        data = list(self.dbclient[self.config.db_name]
                    ["project"].find({}, {'_id': 0, 'CODIGO': 1}))
        response = self.app.response_class(
            response=self.json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response
