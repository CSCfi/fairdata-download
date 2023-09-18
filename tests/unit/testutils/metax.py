import os
from testutils.misc import ResponseMock

class MetaxDatasetResponse(ResponseMock):
    def __init__(self, *args):
        # arg[0] = test data file basename
        # arg[1] = HTTP response code
        ResponseMock.__init__(self, 'tests/unit/test_data/metax_api/v%d/dataset/%s.json' % (int(os.environ.get('METAX_VERSION', 1)), args[0]), args[1])

class MetaxDatasetFilesResponse(ResponseMock):
    def __init__(self, *args):
        # arg[0] = test data file basename
        # arg[1] = HTTP response code
        ResponseMock.__init__(self, 'tests/unit/test_data/metax_api/v%d/files/%s.json' % (int(os.environ.get('METAX_VERSION', 1)), args[0]), args[1])
