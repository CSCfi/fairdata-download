# --------------------------------------------------------------------------------
# Copyright (C) 2024 Ministry of Education and Culture, Finland
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
# License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# @author   CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# @license  GNU Affero General Public License, version 3
# @link     https://research.csc.fi/
# --------------------------------------------------------------------------------

import requests
import unittest
import time
import os
import sys
import socket
from download.utils import BearerAuth
from tests.common.utils import *


class TestDownload(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("=== tests/datasets/test_download")

    def setUp(self):

        print("(initializing)")

        self.token_auth = BearerAuth(os.environ['TRUSTED_SERVICE_TOKEN'])

        # keep track of success, for reference in tearDown
        self.success = False

        # timeout in seconds when waiting for actions to complete
        self.timeout = 120

        self.hostname = os.environ.get('DOWNLOAD_HOST', socket.getfqdn())

        if int(os.environ['METAX_VERSION']) >= 3:
            self.metax_headers = { 'Authorization': 'Token %s' % os.environ['METAX_PASS'] }
        else:
            self.metax_user_auth = (os.environ['METAX_USER'], os.environ['METAX_PASS'])

        self.ida_project = os.environ['IDA_TEST_PROJECT']
        self.ida_user = os.environ['IDA_TEST_USER']
        self.ida_user_auth = (self.ida_user, os.environ['IDA_TEST_PASS'])
        self.ida_admin = os.environ['IDA_ADMIN_USER']
        self.ida_admin_auth = (self.ida_admin, os.environ['IDA_ADMIN_PASS'])

        flush_ida(self)
        flush_metax(self)
        flush_download(self)
        upload_test_data(self)


    def tearDown(self):

        # flush all test data, but only if all tests passed, else leave projects and data
        # as-is so test project state can be inspected

        if self.success:

            print("(cleaning)")

            flush_ida(self)
            flush_metax(self)
            flush_download(self)

        self.assertTrue(self.success)


    def test_download(self):

        """
        Overview:

        1. The test project and user account will be created and initialized as usual.

        2. Project A will have one folder frozen, and the tests will wait until all postprocessing
           has completed such that all metadata is recorded in Metax.

        3. A dataset will be created in Metax, with files included from the frozen folder.

        4. The download service will be tested based on the defined dataset, requesting generation of
           multiple packages, both for full and partial dataset, listings of pending and available
           package generation, retrieval of authorization tokens, and download of individual files.
        """

        print("Freezing folder")
        data = {"project": self.ida_project, "pathname": "/testdata"}
        response = requests.post("%s/apps/ida/api/freeze" % os.environ['IDA_URL'], json=data, auth=self.ida_user_auth, verify=False)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        action_data = response.json()
        self.assertEqual(action_data["action"], "freeze")
        self.assertEqual(action_data["project"], data["project"])
        self.assertEqual(action_data["pathname"], data["pathname"])

        wait_for_pending_actions(self)
        check_for_failed_actions(self)

        print("Retrieving frozen file details for all files associated with freeze action of folder")
        response = requests.get("%s/apps/ida/api/files/action/%s" % (os.environ["IDA_URL"], action_data["pid"]), auth=self.ida_user_auth, verify=False)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        files = response.json()
        self.assertEqual(len(files), 10)

        print("Creating dataset containing all files in scope of frozen folder")
        if int(os.environ['METAX_VERSION']) >= 3:
            dataset_data = DATASET_TEMPLATE_V3
            dataset_data['metadata_owner']['user'] = self.ida_user
            dataset_data['title'] = DATASET_TITLES[0]
            dataset_data['fileset'] = {
                "storage_service": "ida",
                "csc_project": self.ida_project,
                "directory_actions": [
                    {
                        "action": "add",
                        "pathname": "/testdata/"
                    }
                ]
            }
            response = requests.post("%s/datasets" % os.environ['METAX_URL'], headers=self.metax_headers, json=dataset_data)
        else:
            dataset_data = DATASET_TEMPLATE_V1
            dataset_data['metadata_provider_user'] = self.ida_user
            dataset_data['research_dataset']['title'] = DATASET_TITLES[0]
            dataset_data['research_dataset']['files'] = build_dataset_files(self, files)
            url = "%s/rest/v1/datasets" % os.environ['METAX_URL']
            response = requests.post(url, json=dataset_data, auth=self.metax_user_auth)
        self.assertEqual(response.status_code, 201, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        dataset = response.json()
        if int(os.environ['METAX_VERSION']) >= 3:
            dataset_id_1 = dataset.get('id')
        else:
            dataset_id_1 = dataset.get('identifier')
        self.assertIsNotNone(dataset_id_1)

        print("Creating dataset containing all files in scope of frozen baseline/ subfolder")
        if int(os.environ['METAX_VERSION']) >= 3:
            dataset_data = DATASET_TEMPLATE_V3
            dataset_data['metadata_owner']['user'] = self.ida_user
            dataset_data['title'] = DATASET_TITLES[1]
            dataset_data['fileset'] = {
                "storage_service": "ida",
                "csc_project": self.ida_project,
                "directory_actions": [
                    {
                        "action": "add",
                        "pathname": "/testdata/baseline/"
                    }
                ]
            }
            response = requests.post("%s/datasets" % os.environ['METAX_URL'], headers=self.metax_headers, json=dataset_data)
        else:
            files_2 = []
            for file in files:
                if file["pathname"].startswith("/testdata/baseline/"):
                    files_2.append(file)
            dataset_data = DATASET_TEMPLATE_V1
            dataset_data['metadata_provider_user'] = self.ida_user
            dataset_data['research_dataset']['title'] = DATASET_TITLES[1]
            dataset_data['research_dataset']['files'] = build_dataset_files(self, files_2)
            url = "%s/rest/v1/datasets" % os.environ['METAX_URL']
            response = requests.post(url, json=dataset_data, auth=self.metax_user_auth)
        self.assertEqual(response.status_code, 201, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        dataset = response.json()
        if int(os.environ['METAX_VERSION']) >= 3:
            dataset_id_2 = dataset.get('id')
        else:
            dataset_id_2 = dataset.get('identifier')
        self.assertIsNotNone(dataset_id_2)

        # --------------------------------------------------------------------------------

        print("Verify that no active package generation requests exist for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Authorize individual dataset file download")
        file = files[0]
        filename = file.get('pathname')
        self.assertIsNotNone(filename)
        data = { "dataset": dataset_id_1, "file": filename }
        url = "https://%s:4431/authorize" % self.hostname
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        token = response.json().get('token')
        self.assertIsNotNone(token)

        print("Download individual dataset file using authorization token")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        self.assertEqual(response.status_code, 200)

        print("Attempt to download individual dataset file using authorization token a second time")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        self.assertEqual(response.status_code, 401, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Request generation of complete dataset package")
        data = { "dataset": dataset_id_1, "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, response.content.decode(sys.stdout.encoding))
        self.assertEqual(response_json.get('status'), 'PENDING', response.content.decode(sys.stdout.encoding))

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        id = task["id"]
        task_id = task["task_id"]
        self.assertEqual(task["dataset"], dataset_id_1)
        self.assertEqual(task["status"], "NEW")
        self.assertEqual(len(task["scope"]), 0)
        self.assertTrue(task_id.startswith("fd_test_download_project "))

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        tasks = reload_queue(self)
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        self.assertEqual(task["id"], id)
        self.assertNotEqual(task["task_id"], task_id)

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 0)

        wait_for_pending_requests(self, dataset_id_1)

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        print("Verify complete dataset package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'SUCCESS')
        package = response_json.get('package')
        self.assertIsNotNone(package)

        print("Verify complete dataset package exists in cache")
        cmd = "%s/utils/package-stats %s 2>&1 >/dev/null" % (os.environ["ROOT"], package)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Authorize complete dataset package download")
        data = { "dataset": dataset_id_1, "package": package }
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        token = response.json().get('token')
        self.assertIsNotNone(token)

        print("Download complete dataset package using authorization token")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        self.assertEqual(response.status_code, 200)

        print("Request generation of a partial dataset package")
        data = { "dataset": dataset_id_1, "scope": [ "/testdata/test01.dat", "/testdata/test03.dat", "/testdata/test05.dat" ], "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        partial = response_json.get('partial')
        self.assertIsNotNone(partial)
        self.assertEqual(len(partial), 1)
        self.assertEqual(partial[0].get('status'), 'PENDING', response.content.decode(sys.stdout.encoding))

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        id = task["id"]
        task_id = task["task_id"]
        scope = task["scope"]
        self.assertEqual(task["dataset"], dataset_id_1)
        self.assertEqual(task["status"], "NEW")
        self.assertEqual(len(scope), 3)
        self.assertTrue(task_id.startswith("fd_test_download_project "))
        self.assertTrue("/testdata/test01.dat" in scope)
        self.assertTrue("/testdata/test03.dat" in scope)
        self.assertTrue("/testdata/test05.dat" in scope)

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        tasks = reload_queue(self)
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        self.assertEqual(task["id"], id)
        self.assertNotEqual(task["task_id"], task_id)

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 0)

        wait_for_pending_requests(self, dataset_id_1)

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        print("Verify partial dataset package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        partial = response_json.get('partial')
        self.assertIsNotNone(partial)
        self.assertEqual(len(partial), 1)
        self.assertEqual(partial[0].get('status'), 'SUCCESS')
        scope = partial[0].get('scope')
        self.assertIsNotNone(scope)
        self.assertEqual(len(scope), 3)
        self.assertTrue("/testdata/test01.dat" in scope)
        self.assertTrue("/testdata/test03.dat" in scope)
        self.assertTrue("/testdata/test05.dat" in scope)
        package = partial[0].get('package')
        self.assertIsNotNone(package)

        print("Verify partial dataset package exists in cache")
        cmd = "%s/utils/package-stats %s 2>&1 >/dev/null" % (os.environ["ROOT"], package)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Authorize partial dataset package download")
        data = { "dataset": dataset_id_1, "package": package }
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        token = response.json().get('token')
        self.assertIsNotNone(token)

        print("Download partial dataset package using authorization token")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        self.assertEqual(response.status_code, 200)

        if not make_ida_offline(self):
            print("Warning: Failed to put IDA into offline mode. Skipping remaining tests...")
            self.success = True
            return

        print("Authorize individual dataset file download")
        data = { "dataset": dataset_id_1, "file": filename }
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        token = response.json().get('token')
        self.assertIsNotNone(token)

        print("Attempt to download individual dataset file while IDA service is offline")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        self.assertEqual(response.status_code, 503, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Authorize existing dataset package download")
        data = { "dataset": dataset_id_1, "package": package }
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        token = response.json().get('token')
        self.assertIsNotNone(token)

        print("Download existing dataset package while IDA service is offline")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        #self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        self.assertEqual(response.status_code, 200, response.status_code)

        # ---

        flush_cache(self)

        tasks = get_new_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        tasks = get_pending_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        tasks = get_success_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        tasks = get_failed_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        tasks = get_retry_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        print("Request generation of complete dataset package")
        data = { "dataset": dataset_id_1, "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, response.content.decode(sys.stdout.encoding))
        self.assertEqual(response_json.get('status'), 'PENDING', response.content.decode(sys.stdout.encoding))

        print("Verify that package generation request is pending")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'PENDING', response.content.decode(sys.stdout.encoding))

        print("Subscribe to notification of generation of dataset package")
        data = { "dataset": dataset_id_1, "subscriptionData": "abcdef", "notifyURL": "https://%s:4431/mock_notify" % socket.gethostname() }
        notification_file = "%s/mock_notifications/abcdef" % os.environ['DOWNLOAD_CACHE_DIR']
        response = requests.post("https://%s:4431/subscribe" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 201, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, response.content.decode(sys.stdout.encoding))

        print("(sleeping...)")
        time.sleep(5)

        print("Verify that package generation request is still pending")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'PENDING', response.content.decode(sys.stdout.encoding))

        self.assertTrue(make_ida_online(self))

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        id = task["id"]
        task_id = task["task_id"]
        self.assertEqual(task["dataset"], dataset_id_1)
        self.assertEqual(task["status"], "NEW")
        self.assertEqual(len(task["scope"]), 0)
        self.assertTrue(task_id.startswith("fd_test_download_project "))

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        tasks = reload_queue(self)
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        self.assertEqual(task["id"], id)
        self.assertNotEqual(task["task_id"], task_id)

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 0)

        wait_for_pending_requests(self, dataset_id_1)

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        print("Verify complete dataset package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'SUCCESS')
        package = response_json.get('package')
        self.assertIsNotNone(package)

        print("Verify complete dataset package exists in cache")
        cmd = "%s/utils/package-stats %s 2>/dev/null >/dev/null" % (os.environ["ROOT"], package)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Verifying subscribed notification of completed dataset package generation was received")
        self.assertTrue(os.path.exists(notification_file))

        print("Authorize complete dataset package download")
        package = response_json.get('package')
        data = { "dataset": dataset_id_1, "package": package }
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        token = response.json().get('token')
        self.assertIsNotNone(token)

        print("Download complete dataset package using authorization token")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        self.assertEqual(response.status_code, 200)

        print("Authorize individual dataset file download")
        data = { "dataset": dataset_id_1, "file": filename }
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        token = response.json().get('token')
        self.assertIsNotNone(token)

        print("Download individual dataset file using authorization token")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        self.assertEqual(response.status_code, 200)

        # ---

        flush_cache(self)

        tasks = get_new_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        tasks = get_pending_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        tasks = get_success_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        tasks = get_failed_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        tasks = get_retry_tasks(self)
        self.assertIsNotNone(tasks)
        self.assertTrue(isinstance(tasks, list))
        self.assertEqual(len(tasks), 0)

        print("Request generation of multiple dataset packages for multiple datasets")

        data = { "dataset": dataset_id_1, "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        #print(json.dumps(response.json(), indent=4))

        data = { "dataset": dataset_id_1, "scope": [ "/testdata/test01.dat" ], "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        #print(json.dumps(response.json(), indent=4))

        data = { "dataset": dataset_id_1, "scope": [ "/testdata/test03.dat" ], "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        #print(json.dumps(response.json(), indent=4))

        data = { "dataset": dataset_id_1, "scope": [ "/testdata/test05.dat" ], "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        #print(json.dumps(response.json(), indent=4))

        data = { "dataset": dataset_id_2, "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        #print(json.dumps(response.json(), indent=4))

        data = { "dataset": dataset_id_2, "scope": [ "/testdata/baseline/test01.dat" ], "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        #print(json.dumps(response.json(), indent=4))

        data = { "dataset": dataset_id_2, "scope": [ "/testdata/baseline/test03.dat" ], "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        #print(json.dumps(response.json(), indent=4))

        data = { "dataset": dataset_id_2, "scope": [ "/testdata/baseline/test05.dat" ], "testing": True }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        #print(json.dumps(response.json(), indent=4))

        tasks = get_new_tasks(self)
        #print(json.dumps(tasks, indent=4))
        self.assertEqual(len(tasks), 8)
        id_1_1 = tasks[0]["id"]
        task_id_1_1 = tasks[0]["task_id"]
        self.assertEqual(tasks[0]["dataset"], dataset_id_1)
        self.assertEqual(len(tasks[0]["scope"]), 0)
        id_1_2 = tasks[1]["id"]
        task_id_1_2 = tasks[1]["task_id"]
        self.assertEqual(tasks[1]["dataset"], dataset_id_1)
        self.assertEqual(len(tasks[1]["scope"]), 1)
        self.assertEqual(tasks[1]["scope"][0], "/testdata/test01.dat")
        id_1_3 = tasks[2]["id"]
        task_id_1_3 = tasks[2]["task_id"]
        self.assertEqual(tasks[2]["dataset"], dataset_id_1)
        self.assertEqual(len(tasks[2]["scope"]), 1)
        self.assertEqual(tasks[2]["scope"][0], "/testdata/test03.dat")
        id_1_4 = tasks[3]["id"]
        task_id_1_4 = tasks[3]["task_id"]
        self.assertEqual(tasks[3]["dataset"], dataset_id_1)
        self.assertEqual(len(tasks[3]["scope"]), 1)
        self.assertEqual(tasks[3]["scope"][0], "/testdata/test05.dat")
        id_2_1 = tasks[4]["id"]
        task_id_2_1 = tasks[4]["task_id"]
        self.assertEqual(tasks[4]["dataset"], dataset_id_2)
        self.assertEqual(len(tasks[4]["scope"]), 0)
        id_2_2 = tasks[5]["id"]
        task_id_2_2 = tasks[5]["task_id"]
        self.assertEqual(tasks[5]["dataset"], dataset_id_2)
        self.assertEqual(len(tasks[5]["scope"]), 1)
        self.assertEqual(tasks[5]["scope"][0], "/testdata/baseline/test01.dat")
        id_2_3 = tasks[6]["id"]
        task_id_2_3 = tasks[6]["task_id"]
        self.assertEqual(tasks[6]["dataset"], dataset_id_2)
        self.assertEqual(len(tasks[6]["scope"]), 1)
        self.assertEqual(tasks[6]["scope"][0], "/testdata/baseline/test03.dat")
        id_2_4 = tasks[7]["id"]
        task_id_2_4 = tasks[7]["task_id"]
        self.assertEqual(tasks[7]["dataset"], dataset_id_2)
        self.assertEqual(len(tasks[7]["scope"]), 1)
        self.assertEqual(tasks[7]["scope"][0], "/testdata/baseline/test05.dat")

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        print("Re-populate generation queue and verify correct processing order of pending generation tasks - round 1")

        tasks = reload_queue(self)
        #print(json.dumps(tasks, indent=4))
        self.assertEqual(len(tasks), 2)

        self.assertEqual(tasks[0]["dataset"], dataset_id_1)
        self.assertEqual(tasks[0]["id"], id_1_1)
        self.assertEqual(tasks[0]["status"], "PENDING")
        self.assertNotEqual(tasks[0]["task_id"], task_id_1_1)
        self.assertEqual(len(tasks[0]["scope"]), 0)
        self.assertEqual(tasks[1]["dataset"], dataset_id_2)
        self.assertEqual(tasks[1]["id"], id_2_1)
        self.assertEqual(tasks[1]["status"], "PENDING")
        self.assertNotEqual(tasks[1]["task_id"], task_id_2_1)
        self.assertEqual(len(tasks[1]["scope"]), 0)

        wait_for_pending_requests(self, dataset_id_1)
        wait_for_pending_requests(self, dataset_id_2)

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 6)

        print("Re-populate generation queue and verify correct processing order of pending generation tasks - round 2")

        tasks = reload_queue(self)
        #print(json.dumps(tasks, indent=4))
        self.assertEqual(len(tasks), 2)

        self.assertEqual(tasks[0]["dataset"], dataset_id_1)
        self.assertEqual(tasks[0]["id"], id_1_2)
        self.assertEqual(tasks[0]["status"], "PENDING")
        self.assertNotEqual(tasks[0]["task_id"], task_id_1_2)
        self.assertEqual(len(tasks[0]["scope"]), 1)
        self.assertEqual(tasks[0]["scope"][0], "/testdata/test01.dat")
        self.assertEqual(tasks[1]["dataset"], dataset_id_2)
        self.assertEqual(tasks[1]["id"], id_2_2)
        self.assertEqual(tasks[1]["status"], "PENDING")
        self.assertNotEqual(tasks[1]["task_id"], task_id_2_2)
        self.assertEqual(len(tasks[1]["scope"]), 1)
        self.assertEqual(tasks[1]["scope"][0], "/testdata/baseline/test01.dat")

        wait_for_pending_requests(self, dataset_id_1)
        wait_for_pending_requests(self, dataset_id_2)

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 4)

        print("Re-populate generation queue and verify correct processing order of pending generation tasks - round 3")

        tasks = reload_queue(self)
        #print(json.dumps(tasks, indent=4))
        self.assertEqual(len(tasks), 2)

        self.assertEqual(tasks[0]["dataset"], dataset_id_1)
        self.assertEqual(tasks[0]["id"], id_1_3)
        self.assertEqual(tasks[0]["status"], "PENDING")
        self.assertNotEqual(tasks[0]["task_id"], task_id_1_3)
        self.assertEqual(len(tasks[0]["scope"]), 1)
        self.assertEqual(tasks[0]["scope"][0], "/testdata/test03.dat")
        self.assertEqual(tasks[1]["dataset"], dataset_id_2)
        self.assertEqual(tasks[1]["id"], id_2_3)
        self.assertEqual(tasks[1]["status"], "PENDING")
        self.assertNotEqual(tasks[1]["task_id"], task_id_2_3)
        self.assertEqual(len(tasks[1]["scope"]), 1)
        self.assertEqual(tasks[1]["scope"][0], "/testdata/baseline/test03.dat")

        wait_for_pending_requests(self, dataset_id_1)
        wait_for_pending_requests(self, dataset_id_2)

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 2)

        print("Re-populate generation queue and verify correct processing order of pending generation tasks - round 4")

        tasks = reload_queue(self)
        #print(json.dumps(tasks, indent=4))
        self.assertEqual(len(tasks), 2)

        self.assertEqual(tasks[0]["dataset"], dataset_id_1)
        self.assertEqual(tasks[0]["id"], id_1_4)
        self.assertNotEqual(tasks[0]["task_id"], task_id_1_4)
        self.assertEqual(tasks[0]["status"], "PENDING")
        self.assertEqual(len(tasks[0]["scope"]), 1)
        self.assertEqual(tasks[0]["scope"][0], "/testdata/test05.dat")
        self.assertEqual(tasks[1]["dataset"], dataset_id_2)
        self.assertEqual(tasks[1]["id"], id_2_4)
        self.assertNotEqual(tasks[1]["task_id"], task_id_2_4)
        self.assertEqual(tasks[1]["status"], "PENDING")
        self.assertEqual(len(tasks[1]["scope"]), 1)
        self.assertEqual(tasks[1]["scope"][0], "/testdata/baseline/test05.dat")

        wait_for_pending_requests(self, dataset_id_1)
        wait_for_pending_requests(self, dataset_id_2)

        tasks = get_new_tasks(self)
        self.assertEqual(len(tasks), 0)

        tasks = get_pending_tasks(self)
        self.assertEqual(len(tasks), 0)

        print("Re-populate generation queue and verify no further generation tasks to be queued")

        tasks = reload_queue(self)
        self.assertEqual(len(tasks), 0)

        # --------------------------------------------------------------------------------
        # If all tests passed, record success, in which case tearDown will be done

        self.success = True

        # --------------------------------------------------------------------------------
        # TODO: consider which tests may be missing...
