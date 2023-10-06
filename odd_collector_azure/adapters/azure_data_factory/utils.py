import json
from datetime import datetime


class ADFMetadataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj.__dict__.copy()
