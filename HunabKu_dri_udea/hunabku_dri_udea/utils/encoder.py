from json import JSONEncoder
from bson import ObjectId

class JsonEncoder(JSONEncoder):
    """
    Custom JSON encoder
    """

    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return JSONEncoder.default(self, o)