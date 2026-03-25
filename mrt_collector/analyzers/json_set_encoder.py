import json


# https://stackoverflow.com/a/8230505/8903959
class JSONSetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

# passing a set to json.dumpraises TypeError, this fixes
# this was originally in mh_export_analyzer but I moved
# here for organizations sake since it's used in multiple places
