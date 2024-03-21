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
import os
import sys
import socket
from download.utils import BearerAuth
from tests.common.utils import *


class TestHousekeeping(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("=== tests/datasets/test_housekeeping")


    def setUp(self):

        print("(initializing)")

        self.token_auth = BearerAuth(os.environ['TRUSTED_SERVICE_TOKEN'])

        # keep track of success, for reference in tearDown
        self.success = False

        # timeout when waiting for actions to complete
        self.timeout = 600 # 10 minutes

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


    def test_housekeeping(self):

        print("-- Initialization")

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

        print("Creating dataset 1 containing all files in scope of frozen folder")
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
            response = requests.post("%s/rest/v1/datasets" % os.environ['METAX_URL'], json=dataset_data, auth=self.metax_user_auth)
        self.assertEqual(response.status_code, 201, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        dataset = response.json()
        if int(os.environ['METAX_VERSION']) >= 3:
            dataset_id_1 = dataset.get('id')
        else:
            dataset_id_1 = dataset.get('identifier')
        self.assertIsNotNone(dataset_id_1)

        print("Creating dataset 2 containing subdirectory of frozen folder")
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
                        "pathname": "/testdata/baseline/"
                    }
                ]
            }
            response = requests.post("%s/datasets" % os.environ['METAX_URL'], headers=self.metax_headers, json=dataset_data)
        else:
            dataset_data = DATASET_TEMPLATE_V1
            dataset_data['metadata_provider_user'] = self.ida_user
            dataset_data['research_dataset']['title'] = DATASET_TITLES[0]
            baseline_files = []
            for file in files:
                if file['pathname'].startswith('/testdata/baseline/'):
                    baseline_files.append(file)
            dataset_data['research_dataset']['files'] = build_dataset_files(self, baseline_files)
            response = requests.post("%s/rest/v1/datasets" % os.environ['METAX_URL'], json=dataset_data, auth=self.metax_user_auth)
        self.assertEqual(response.status_code, 201, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        dataset = response.json()
        if int(os.environ['METAX_VERSION']) >= 3:
            dataset_id_2 = dataset.get('id')
        else:
            dataset_id_2 = dataset.get('identifier')
        self.assertIsNotNone(dataset_id_2)
        self.assertNotEqual(dataset_id_1, dataset_id_2)

        # --------------------------------------------------------------------------------

        print("-- Package older than dataset")

        print("Verify that no active package generation requests exist for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Request generation of complete dataset package")
        data = { "dataset": dataset_id_1 }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, response.content.decode(sys.stdout.encoding))

        wait_for_pending_requests(self, dataset_id_1)

        print("Verify complete dataset package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'SUCCESS')
        package = response_json.get('package')
        self.assertIsNotNone(package)

        print("Verify complete dataset package exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package)
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

        print("Authorize complete dataset package download")
        data = { "dataset": dataset_id_1, "package": package }
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        token = response.json().get('token')
        self.assertIsNotNone(token)

        print("Update generation timestamp of dataset package to be far in the past")
        data = { "package": package, "timestamp": "2000-01-01 00:00:00" }
        response = requests.post("https://%s:4431/update_package_timestamps" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        print(response.content.decode(sys.stdout.encoding))

        print("Attempt to download outdated complete dataset package using authorization token")
        response = requests.get("https://%s:4431/download?token=%s" % (self.hostname, token), auth=self.token_auth)
        self.assertEqual(response.status_code, 409, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Attempt to authorize outdated complete dataset package download")
        data = { "dataset": dataset_id_1, "package": package }
        response = requests.post("https://%s:4431/authorize" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 409, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Verify outdated complete dataset package is no longer listed with available packages for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Verify outdated complete dataset package still exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        old_package = package

        print("Old package: %s" % old_package)

        print("Request generation of new complete dataset package (triggers housekeeping, removing old package)")
        data = { "dataset": dataset_id_1 }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, response.content.decode(sys.stdout.encoding))

        wait_for_pending_requests(self, dataset_id_1)

        print("Verify new complete dataset package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'SUCCESS')
        package = response_json.get('package')
        self.assertIsNotNone(package)

        print("Verify new complete dataset package exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Verify outdated complete dataset package no longer exists in cache (removed by housekeeping)")
        cmd = "%s/utils/package-stats %s 2>/dev/null >/dev/null" % (os.environ["ROOT"], old_package)
        result = os.system(cmd)
        self.assertNotEqual(result, 0)

        flush_cache(self)

        # --------------------------------------------------------------------------------

        print("-- Zero package file size in database")

        print("Verify that no active package generation requests exist for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Request generation of complete dataset package")
        data = { "dataset": dataset_id_1 }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, response.content.decode(sys.stdout.encoding))

        wait_for_pending_requests(self, dataset_id_1)

        print("Verify complete dataset package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'SUCCESS')
        package = response_json.get('package')
        self.assertIsNotNone(package)

        print("Verify complete dataset package exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Update package file size in database to be zero")
        data = { "package": package, "size_bytes": 0 }
        response = requests.post("https://%s:4431/update_package_file_size" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        print(response.content.decode(sys.stdout.encoding))

        print("Run housekeeping to purge now-invalid package")
        response = requests.post("https://%s:4431/housekeep" % self.hostname, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Verify complete dataset package is no longer listed with available packages for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        flush_cache(self)

        # --------------------------------------------------------------------------------

        print("-- Zero package file size on disk")

        print("Verify that no active package generation requests exist for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Request generation of complete dataset package")
        data = { "dataset": dataset_id_1 }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, response.content.decode(sys.stdout.encoding))

        wait_for_pending_requests(self, dataset_id_1)

        print("Verify complete dataset package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'SUCCESS')
        package = response_json.get('package')
        self.assertIsNotNone(package)

        print("Verify complete dataset package exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Update package file size in cache to be zero")
        cmd = "%s/utils/empty-package-file %s >/dev/null 2>&1" % (os.environ["ROOT"], package)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Run housekeeping to purge now-invalid package")
        response = requests.post("https://%s:4431/housekeep" % self.hostname, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Verify complete dataset package is no longer listed with available packages for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        flush_cache(self)

        # --------------------------------------------------------------------------------

        print("-- Dataset no longer in Metax (housekeeping using CLI command)")

        print("Verify that no active package generation requests exist for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Request generation of package for dataset 1")
        data = { "dataset": dataset_id_1 }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_1, response.content.decode(sys.stdout.encoding))

        print("Request generation of package for dataset 2")
        data = { "dataset": dataset_id_2 }
        response = requests.post("https://%s:4431/requests" % self.hostname, json=data, auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertIsNotNone(response_json)
        self.assertEqual(response_json.get('dataset'), dataset_id_2, response.content.decode(sys.stdout.encoding))

        wait_for_pending_requests(self, dataset_id_1)
        wait_for_pending_requests(self, dataset_id_2)

        print("Verify dataset 1 package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'SUCCESS')
        package_1 = response_json.get('package')
        self.assertIsNotNone(package_1)

        print("Verify dataset 2 package is reported in package listing")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_2), auth=self.token_auth)
        self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        response_json = response.json()
        self.assertEqual(response_json.get('status'), 'SUCCESS')
        package_2 = response_json.get('package')
        self.assertIsNotNone(package_2)
        self.assertNotEqual(package_1, package_2)

        print("Verify dataset 1 package file exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package_1)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Verify dataset 2 package file exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package_2)
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Delete datasets from Metax")
        if int(os.environ['METAX_VERSION']) >= 3:
            response = requests.delete("%s/users/%s/data?flush=true" % (os.environ['METAX_URL'], self.ida_user), headers=self.metax_headers)
        else:
            response = requests.post("%s/rpc/v1/datasets/flush_user_data?metadata_provider_user=%s" % (os.environ['METAX_URL'], self.ida_user), auth=self.metax_user_auth)
        self.assertEqual(response.status_code, 204, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Verify dataset 1 no longer in Metax")
        if int(os.environ['METAX_VERSION']) >= 3:
            response = requests.get("%s/datasets/%s" % (os.environ['METAX_URL'], dataset_id_1), headers=self.metax_headers)
        else:
            response = requests.get("%s/rest/v1/datasets/%s" % (os.environ['METAX_URL'], dataset_id_1), auth=self.metax_user_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Verify dataset 2 no longer in Metax")
        if int(os.environ['METAX_VERSION']) >= 3:
            response = requests.get("%s/datasets/%s" % (os.environ['METAX_URL'], dataset_id_2), headers=self.metax_headers)
        else:
            response = requests.get("%s/rest/v1/datasets/%s" % (os.environ['METAX_URL'], dataset_id_2), auth=self.metax_user_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Run housekeeping to purge now-invalid packages for datasets 1 and 2, using CLI command")
        cmd = "%s/cli/cache-cli housekeep >/dev/null 2>&1" % os.environ["ROOT"]
        result = os.system(cmd)
        self.assertEqual(result, 0)

        print("Verify dataset 1 package file no longer exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package_1)
        result = os.system(cmd)
        self.assertNotEqual(result, 0)

        print("Verify dataset 2 package file no longer exists in cache")
        cmd = "%s/utils/package-stats %s >/dev/null 2>&1" % (os.environ["ROOT"], package_2)
        result = os.system(cmd)
        self.assertNotEqual(result, 0)

        print("Verify complete dataset 1 package is no longer listed with available packages for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_1), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        print("Verify complete dataset 2 package is no longer listed with available packages for dataset")
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_id_2), auth=self.token_auth)
        self.assertEqual(response.status_code, 404, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        # --------------------------------------------------------------------------------
        # If all tests passed, record success, in which case tearDown will be done

        self.success = True

        # --------------------------------------------------------------------------------
        # TODO: consider which tests may be missing...
