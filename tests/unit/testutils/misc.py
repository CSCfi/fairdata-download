import json

class Recorder(object):
    called = False

class CeleryTask(object):
    id = 1

class ResponseMock(object):
    def __init__(self, *args):
        if args[0]:
            with open(args[0], "r") as test_data:
                self.body = test_data.read()
        if args[1]:
            self.status_code = args[1]
        else:
            self.status_code = 200

    def json(self):
        return json.loads(self.body)
