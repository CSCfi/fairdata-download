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

import os
import sys
import time
import json
import requests

DATASET_TEMPLATE_V3 = {
    "pid_type": "URN",
    "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
    "metadata_owner": {
        "user": "test_user",
        "organization": "Test Organization"
    },
    "access_rights": {
        "access_type": {
            "url": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
        }
    },
    "creator": [
        {
            "@type": "Person",
            "member_of": {
                "@type": "Organization",
                "name": {
                    "en": "Test Organization"
                }
            },
            "name": "Test User"
        }
    ],
    "description": {
        "en": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
    },
    "title": {
        "en": "Test Dataset"
    },
    "state": "published"
}

DATASET_TEMPLATE_V1 = {
    "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
    "metadata_provider_user": "test_user",
    "metadata_provider_org": "test_organization",
    "research_dataset": {
        "access_rights": {
            "access_type": {
                "identifier": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
            }
        },
        "creator": [
            {
                "@type": "Person",
                "member_of": {
                    "@type": "Organization",
                    "name": {
                        "en": "Test Organization"
                    }
                },
                "name": "Test User"
            }
        ],
        "description": {
            "en": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
        },
        "title": {
            "en": "Test Dataset"
        }
    }
}

DATASET_TITLES = [
    { "en": "Lake Chl-a products from Finland (MERIS, FRESHMON)" },
    { "fi": "MERIVEDEN LÄMPÖTILA POHJALLA (VELMU)" },
    { "sv": "Svenska ortnamn i Finland" },
    { "en": "The Finnish Subcorpus of Topling - Paths in Second Language Acquisition" },
    { "en": "SMEAR data preservation 2019" },
    { "en": "Finnish Opinions on Security Policy and National Defence 2001: Autumn" }
]


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


def flush_ida(self):

    print("Flushing test data from IDA...")

    url = "%s/apps/ida/api/delete" % os.environ['IDA_URL']
    data = {"project": self.ida_project, "pathname": "/testdata"}
    #print("POST %s %s %s" % (json.dumps(self.ida_user_auth), json.dumps(data), url))
    response = requests.post(url, json=data, auth=self.ida_user_auth, verify=False)
    self.assertTrue(response.status_code in [ 200, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

    url = "%s/remote.php/webdav/%s+/testdata" % (os.environ['IDA_URL'], os.environ['IDA_TEST_PROJECT'])
    #print("DELETE %s %s" % (json.dumps(self.ida_user_auth), url))
    response = requests.request("DELETE", url, auth=self.ida_user_auth, verify=False)
    self.assertTrue(response.status_code in [ 200, 204, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

    url = "%s/apps/ida/api/flush?project=%s" % (os.environ['IDA_URL'], os.environ['IDA_TEST_PROJECT'])
    #print("POST %s %s" % (json.dumps(self.ida_admin_auth), url))
    response = requests.post(url, auth=self.ida_admin_auth, verify=False)
    self.assertTrue(response.status_code in [ 200, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

    # TODO: Investigate why this second flush is needed to avoid subsequent freeze action collision (lag in Nextcloud housekeeping?)
    time.sleep(5)
    url = "%s/apps/ida/api/flush?project=%s" % (os.environ['IDA_URL'], os.environ['IDA_TEST_PROJECT'])
    #print("POST %s %s" % (json.dumps(self.ida_admin_auth), url))
    response = requests.post(url, auth=self.ida_admin_auth, verify=False)
    self.assertTrue(response.status_code in [ 200, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))


def flush_metax(self):

    print("Flushing test data from METAX...")

    if int(os.environ['METAX_VERSION']) >= 3:

        url ="%s/files?csc_project=%s&storage_service=ida&flush=true" % (os.environ['METAX_URL'], os.environ['IDA_TEST_PROJECT'])
        #print("DELETE %s %s" % (json.dumps(self.metax_headers), url))
        response = requests.delete(url, headers=self.metax_headers)
        self.assertTrue(response.status_code in [ 200, 204, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        url = "%s/users/%s/data?flush=true" % (os.environ['METAX_URL'], os.environ['IDA_TEST_USER'])
        response = requests.delete(url, headers=self.metax_headers)
        #print("DELETE %s %s" % (json.dumps(self.metax_headers), url))
        self.assertTrue(response.status_code in [ 200, 204, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

    else:

        url = "%s/rpc/v1/files/flush_project?project_identifier=%s" % (os.environ['METAX_URL'], os.environ['IDA_TEST_PROJECT'])
        #print("POST %s %s" % (json.dumps(self.metax_user), url))
        response = requests.post(url, auth=self.metax_user)
        self.assertTrue(response.status_code in [ 200, 204, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

        url = "%s/rpc/v1/datasets/flush_user_data?metadata_provider_user=%s" % (os.environ['METAX_URL'], os.environ['IDA_TEST_USER'])
        #print("POST %s %s" % (json.dumps(self.metax_user), url))
        response = requests.post(url, auth=self.metax_user)
        self.assertTrue(response.status_code in [ 200, 204, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))


def flush_download(self):
    print("Flushing test data from Download Service...")
    cmd = "%s/utils/flush-all" % os.environ['ROOT']
    result = os.system(cmd)
    self.assertEqual(result, 0)


def upload_test_data(self):

    print("Uploading test data to IDA...")

    # Create /testdata folder in staging

    url = "%s/remote.php/webdav/%s+/testdata" % (os.environ['IDA_URL'], os.environ['IDA_TEST_PROJECT'])
    response = requests.request("MKCOL", url, auth=self.ida_user_auth, verify=False)
    self.assertEqual(response.status_code, 201,  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))

    # Upload /testdata/test0[1-5].dat to staging

    for count in [ 1, 2, 3, 4, 5 ]:

        pathname = "testdata/test0%d.dat" % count
        localpath = "%s/tests/%s" % (os.environ['ROOT'], pathname)
        url = "%s/remote.php/webdav/%s+/%s" % (os.environ['IDA_URL'], os.environ['IDA_TEST_PROJECT'], pathname)

        with open(localpath, 'rb') as file:
            response = requests.put(url, data=file, auth=self.ida_user_auth, verify=False)
            self.assertEqual(response.status_code, 201,  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))


def wait_for_pending_requests(self, dataset_pid):
    print("(waiting for requested packages to be generated)")
    max_time = time.time() + self.timeout
    pending = True
    looped = False
    while pending and time.time() < max_time:
        response = requests.get("https://%s:4431/requests?dataset=%s" % (self.hostname, dataset_pid), auth=self.token_auth)
        self.assertTrue(response.status_code in [ 200, 404 ],  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        if response.status_code == 200:
            response_json = response.json()
            status = response_json.get('status')
            pending = status != None and status not in [ 'SUCCESS', 'FAILED' ]
            if not pending:
                partial = response_json.get('partial', [])
                for req in partial:
                    if not pending:
                        pending = req.get('status') not in [ 'SUCCESS', 'FAILED' ]
            if pending:
                looped = True
                print(".", end='', flush=True)
                time.sleep(1)
        else: # 404
            pending = False
    if looped:
        print("")
    self.assertTrue(time.time() < max_time, "Timed out waiting for requested packages to be generated")


def wait_for_pending_actions(self):
    print("(waiting for pending actions to fully complete)")
    print(".", end='', flush=True)
    response = requests.get("%s/apps/ida/api/actions?project=%s&status=pending" % (os.environ['IDA_URL'], self.ida_project), auth=self.ida_user_auth, verify=False)
    self.assertEqual(response.status_code, 200,  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
    actions = response.json()
    max_time = time.time() + self.timeout
    while len(actions) > 0 and time.time() < max_time:
        print(".", end='', flush=True)
        time.sleep(1)
        response = requests.get("%s/apps/ida/api/actions?project=%s&status=pending" % (os.environ['IDA_URL'], self.ida_project), auth=self.ida_user_auth, verify=False)
        self.assertEqual(response.status_code, 200,  "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
        actions = response.json()
    print("")
    self.assertTrue(time.time() < max_time, "Timed out waiting for pending actions to fully complete")


def check_for_failed_actions(self):
    print("(verifying no failed actions)")
    response = requests.get("%s/apps/ida/api/actions?project=%s&status=failed" % (os.environ['IDA_URL'], self.ida_project), auth=self.ida_user_auth, verify=False)
    self.assertEqual(response.status_code, 200, "%s %s" % (response.status_code, response.content.decode(sys.stdout.encoding)[:1000]))
    actions = response.json()
    assert(len(actions) == 0)


def build_dataset_files(self, action_files):
    dataset_files = []
    for action_file in action_files:
        dataset_file = {
            "title": action_file['pathname'],
            "identifier": action_file['pid'],
            "description": "test data file",
            "use_category": { "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source" }
        }
        dataset_files.append(dataset_file)
    return dataset_files


def make_ida_offline(self):
    print("(putting IDA service into offline mode)")
    offline_sentinel_file = "%s/control/OFFLINE" % os.environ["IDA_DATA_ROOT"]
    if os.path.exists(offline_sentinel_file):
        print("(service already in offline mode)")
        return True
    url = "%s/apps/ida/api/offline" % os.environ['IDA_URL']
    response = requests.post(url, auth=self.ida_admin_auth, verify=False)
    if response.status_code == 200:
        print("(service put into offline mode by API request)")
        return True
    # In case the /offline endpoint is not yet deployed, try to create a local file
    if response.status_code != 409:
        cmd = "sudo -u %s touch %s" % (os.environ["HTTPD_USER"], offline_sentinel_file)
        result = os.system(cmd)
        if ((result == 0) and (os.path.exists(offline_sentinel_file))):
            print("(service put into offline mode by local sentinel file creation)")
            return True
    return False


def make_ida_online(self):
    print("(putting IDA service into online mode)")
    offline_sentinel_file = "%s/control/OFFLINE" % os.environ["IDA_DATA_ROOT"]
    if not os.path.exists(offline_sentinel_file):
        print("(service already in online mode)")
        return True
    url = "%s/apps/ida/api/offline" % os.environ['IDA_URL']
    response = requests.delete(url, auth=self.ida_admin_auth, verify=False)
    if response.status_code == 200:
        print("(service put into online mode by API request)")
        return True
    # In case the /offline endpoint is not yet deployed, try to delete any local file
    if response.status_code != 409:
        cmd = "sudo -u %s rm -f %s" % (os.environ["HTTPD_USER"], offline_sentinel_file)
        result = os.system(cmd)
        if ((result == 0) and (not os.path.exists(offline_sentinel_file))):
            print("(service put into online mode by local sentinel file deletion)")
            return True
    return False

