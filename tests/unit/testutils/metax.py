from testutils.misc import ResponseMock

class MetaxDatasetResponse(ResponseMock):
    def __init__(self, *args):
        ResponseMock.__init__(self, 'tests/unit/test_data/metax_api/dataset/%s.json' % args[0], args[1])

class MetaxDatasetFilesResponse(ResponseMock):
    def __init__(self, *args):
        ResponseMock.__init__(self, 'tests/unit/test_data/metax_api/files/%s.json' % args[0], args[1])
