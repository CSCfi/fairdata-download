"""
    download.events
    ~~~~~~~~~~~~~~~~~~

    Utility module for constructing Fairdata Download Service download events for publication to metrics.fairdata.fi
"""
import re
from dateutil.parser import parse


def normalize_timestamp(timestamp):
    """
    Returns the input timestamp string as a normalized ISO UTC timestamp YYYY-MM-DDThh:mm:ssZ
    """
    try:
        return parse(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")
    except TypeError:
        return None


def normalize_event_pathname(pathname):
    """
    Takes a file system pathname and normalizes it into a single token acceptable as part of an event title
    """

    if not pathname or pathname == "":
        return "__"

    # remove initial/final whitespace
    token = pathname.strip()

    # remove initial/final forward slashes
    token = re.sub("^\/+", "", token)
    token = re.sub("\/+$", "", token)

    # map one or more characters other than word characters, digits, full stops, or forward slashes to single underscores
    token = re.sub("[^\w\d\.\/]+", "_", token)

    # merge multiple adjacent underscores to single underscores
    token = re.sub("_+", "_", token)

    # remove underscores adjacent to forward slashes
    token = re.sub("_*\/_*", "/", token)

    # map one or more adjacent forward slashes to double underscores
    token = re.sub("\/+", "__", token)

    # remove initial/final underscores
    token = re.sub("^_+", "", token)
    token = re.sub("_+$", "", token)

    # ensure non-null token
    if token == "":
        token = "__"

    return token


def normalize_event_scope(scope):
    """
    Takes a scope array and normalizes it into a single token acceptable as part of an event title
    """

    if not scope or len(scope)< 1:
        return "__"

    scope_tokens = []

    for pathname in scope:
        scope_tokens.append(normalize_event_pathname(pathname))

    scope_tokens.sort()

    token = ""
    first = True

    for scope_token in scope_tokens:
        if not first:
            # join multiple scope pathnames with commas
            token = "%s," % token
        token = "%s%s" % (token, scope_token)
        first = False

    return token


def construct_event_title(event):

    event_type = event["type"]

    title = "%s / %s" % (event["dataset"], event_type)

    if event_type == "FILE":
        title = "%s / %s" % (title, normalize_event_pathname(event["file"]))
    elif event_type == "PARTIAL":
        title = "%s / %s" % (title, normalize_event_scope(event["scope"]))
    elif event_type == "PACKAGE":
        title = "%s / %s" % (title, normalize_event_pathname(event["package"]))
    
    title = "%s / %s " % (title, event["status"])

    return title
