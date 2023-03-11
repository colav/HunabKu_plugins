from hunabku.HunabkuBase import HunabkuPluginBase, endpoint
from hunabku.Config import Config, Param
from flask import redirect
from pymongo import MongoClient
import validators
from bson.objectid import ObjectId


class Shortener(HunabkuPluginBase):
    config = Config()
    config += Param(db_uri="mongodb://localhost:27017/",
                    doc="MongoDB string connection")

    config += Param(db_name="urlshortener",
                    doc="Mongo DB name")

    config += Param(collection_name="records",
                    doc="Mongo DB collection name to save the records")

    def __init__(self, hunabku):
        super().__init__(hunabku)
        self.dbclient = MongoClient(self.config.db_uri)
        self.db = self.dbclient[self.config.db_name]
        self.collection = self.db[self.config.collection_name]

    def validate_url(self, url):
        validation = validators.url(url)
        if validation:
            return True
        else:
            return False

    @endpoint('/shorturl/<url_id>', methods=['GET', 'POST'])
    def url_id_end(self, url_id):
        """
        @api {get} /shorturl/<url_id> Url resolver
        @apiDescription redirects to an url given the url id
        @apiName resolver
        @apiGroup UrlShortener

        @apiSuccess  redirect to the website 
        """
        x = self.collection.find_one({"_id": ObjectId(url_id)})
        if x:
            return redirect(x["url"])
        else:
            response = self.app.response_class(
                response=self.json.dumps({"error": "urlid not found"}),
                status=404,
                mimetype='application/json'
            )
            return response

    @endpoint('/shorturl_create', methods=['GET', 'POST'])
    def url_create_end(self):
        """
        @api {get} /shorturl_create Url create
        @apiDescription creates an url id
        @apiName create
        @apiGroup UrlShortener

        @apiParam {String} apikey  Credential for authentication
        @apiParam {String} url  url to encode

        @apiSuccess  redirect to the website 
        """
        if not self.valid_apikey():
            return self.apikey_error()
        url = ""
        args = {}
        if self.request.method == 'POST':
            args = self.request.form
        else:
            args = self.request.args
        if not self.valid_parameters(["url", "apikey"]):
            return self.badrequest_error()

        if not self.validate_url(args.get("url")):
            response = self.app.response_class(
                response=self.json.dumps(
                    {"error": "Bad request, invalid URL"}),
                status=400,
                mimetype='application/json'
            )
            return response
        url = args["url"]
        x = self.collection.insert_one({"url": url})
        data = {"urlid": str(x.inserted_id)}
        response = self.app.response_class(
            response=self.json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response
