import os
import json
import sys
import requests
import string
import random

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def construct_event_title(event):
    dataset = event["dataset"]
    chars = string.ascii_lowercase + string.digits
    access_id = f"xxx_{''.join(random.choices(chars, k=6))}"
    return f"{dataset} / ACCESS / {access_id}"

def publish_event(event):
    try:
        title = construct_event_title(event)
        timestamp = event["started"]

        # Limit events to those with a timestamp older than a configured milestone, corresponding
        # to when the old download service was updated to automatically report emulated ACCESS events,
        # to newly reported events here are limited to true legacy events and don't duplicate events
        # after the update milestone.
        cutoff = os.environ.get("LEGACY_EVENTS_CUTOFF_TIMESTAMP", "2222-22-22T22:22:22Z")
        if timestamp >= cutoff:
            return

        api = os.environ["FDWE_API"]
        token = os.environ["FDWE_TOKEN"]
        environment = os.environ.get("ENVIRONMENT", "DEV")
        print(f"{environment} {timestamp} {title}")

        # TEMP DEBUG
        return

        url = "%s/report?token=%s&environment=%s&service=DOWNLOAD&scope=%s&timestamp=%s" % (api, token, environment, title, timestamp)
        response = requests.post(url, verify=False)
        if response.status_code != 200:
            raise Exception(response.text)
    except BrokenPipeError as error:
        raise error
    except BaseException as error:
        print("Error publishing event: %s" % str(error), file=sys.stderr)
        os.abort()

if not "EVENTS_SNAPSHOT_FILE" in os.environ:
    print("Error: 'EVENTS_SNAPSHOT_FILE' is not defined in environment", file=sys.stderr)
    os.abort()

events_file = os.environ.get("EVENTS_SNAPSHOT_FILE")

if not os.path.isfile(events_file):
    print("Error: Could not find events file '%s'" % events_file, file=sys.stderr)
    os.abort()

print("Loading events file: %s" % events_file, file=sys.stderr)

with open(events_file) as file:
    events = json.load(file)

print("Loaded %s events" % str(len(events)), file=sys.stderr)

try:
    for event in events:
        publish_event(event)
except BrokenPipeError as error:
    pass
