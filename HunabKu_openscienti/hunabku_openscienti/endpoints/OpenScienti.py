from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from pymongo import MongoClient
import sys


class OpenScienti(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")
    config += Param(db_name="yuku",
                    doc="MongoDB Open Scienti generated database by yuku")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)
        self.db = self.dbclient[self.config.db_name]

    @endpoint('/openscienti/cvlac', methods=['GET'])
    def openscienti_cvlac(self):
        """
        @api {get} /openscienti/cvlac OpenScienti cvlac endpoint
        @apiName cvlac
        @apiGroup OpenScienti
        @apiDescription Allows to perform queries for cvlac users, given the COD_RH

        @apiParam {String} COD_RH  User primary key

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # all the info for the user
            curl -i http://apis.colav.co/openscienti/cvlac?COD_RH=0000000020
        """
        try:
            cod_rh = self.request.args.get('COD_RH')
            if cod_rh:
                data = {}
                data["raw_data"] = list(self.db["cvlac_data"].find(
                    {'id_persona_pr': cod_rh}, {'_id': 0}))
                data["scrapped_data"] = list(self.db["cvlac_stage"].find(
                    {'id_persona_pr': cod_rh}, {"_id": 0}))
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                return response
            else:
                #  return all the records
                data = {}
                data["raw_data"] = list(
                    self.db["cvlac_data"].find({}, {'_id': 0}))
                data["scrapped_data"] = list(
                    self.db["cvlac_stage"].find({}, {"_id": 0}))
                response = self.app.response_class(
                    response=self.json.dumps(data),
                    status=200,
                    mimetype='application/json'
                )
                return response

            data = {
                "error": "Bad Request", "message": "invalid parameters, please select privide COD_RH  parameter."}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response
        except Exception as e:
            data = {"error": "Bad Request", "message": str(
                sys.exc_info()), "execption": str(e)}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response

    @endpoint('/openscienti/info', methods=['GET'])
    def openscienti_info(self):
        """
        @api {get} /openscienti/info OpenScienti info endpoint
        @apiName Info
        @apiGroup OpenScienti
        @apiDescription Allows to perform queries for information,
                        about avialable cvlac and ids.
        @apiParam {String} get Options are resume and ids, ids require additional parameters model_year and institution

        @apiSuccess {Object}  Resgisters from MongoDB in Json format.

        @apiError (Error 401) msg  The HTTP 401 Unauthorized invalid authentication apikey for the target resource.
        @apiError (Error 400) msg  Bad request, if the query is not right.

        @apiExample {curl} Example usage:
            # resume of open scienti data
            curl -i http://apis.colav.co/scienti/info
        """
        try:
            data = {}
            data["ids"] = self.db["cvlac_data"].distinct("id_persona_pr")
            data["dataset_info"] = self.db["cvlac_dataset_info"].find_one({}, {
                                                                          "_id": 0})
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=200,
                mimetype='application/json'
            )
            return response

        except Exception as e:
            data = {"error": "Bad Request", "message": str(
                sys.exc_info()), "exception": str(e)}
            response = self.app.response_class(
                response=self.json.dumps(data),
                status=400,
                mimetype='application/json'
            )
            return response
