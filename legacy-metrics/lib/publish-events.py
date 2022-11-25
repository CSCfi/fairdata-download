import os
import json
import sys
import requests
from events import construct_event_title

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def publish_event(event):
    try:
        title = construct_event_title(event)
        timestamp = event["started"]

        # Normalize timestamps to noon on the original date of the event to ensure
        # that the event is recorded and later reported per that actual date no matter
        # how timezones might disagree between the operating system, Matomo configuration,
        # database configuration, etc. (easier and far more reliable than tweaking all
        # of the various configurations that may be in play).
        fields = timestamp.split("T")
        timestamp = "%sT12:00:00Z" % fields[0]

        print("%s: %s" % (timestamp, title))

        api = os.environ["FDWE_API"]
        token = os.environ["FDWE_TOKEN"]
        environment = os.environ.get("ENVIRONMENT", "DEV")

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
