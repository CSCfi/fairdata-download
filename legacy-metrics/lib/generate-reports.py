import os
import json
import sys
import operator
from events import construct_event_title

if not "EVENTS_SNAPSHOT_FILE" in os.environ:
    print("Error: 'EVENTS_SNAPSHOT_FILE' is not defined in environment", file=sys.stderr)
    os.abort()

events_file = os.environ.get("EVENTS_SNAPSHOT_FILE")

if not os.path.isfile(events_file):
    print("Error: Could not find events file '%s'" % events_file, file=sys.stderr)
    os.abort()

if not "EVENTS_REPORT_ROOT" in os.environ:
    print("Error: 'EVENTS_REPORT_ROOT' is not defined in environment", file=sys.stderr)
    os.abort()

reports_root = os.environ.get("EVENTS_REPORT_ROOT")

if not os.path.isdir(reports_root):
    print("Error: Could not find reports root folder '%s'" % reports_root, file=sys.stderr)
    os.abort()

print("Loading events file: %s" % events_file, file=sys.stderr)

with open(events_file) as file:
    events = json.load(file)

print("Loaded %s events" % str(len(events)), file=sys.stderr)

#--------------------------------------------------------------------------------

events_by_month = {}
totals_by_month = {}
totals_to_date  = {}

#--------------------------------------------------------------------------------


def record_event_by_month(extracted_event):
    title = construct_event_title(extracted_event)
    timestamp = extracted_event["started"]
    fields = timestamp.split("-")
    year_month = "%s-%s" % (fields[0], fields[1])
    monthly_events = events_by_month.get(year_month, [])
    monthly_events.append(title)
    events_by_month[year_month] = monthly_events


def updateDownloadTotals(download_totals, type, result): 
    """
    Updates a collection of download totals based on reported event details
    """

    total_requests = download_totals.get("total_requests", 0)
    total_successful = download_totals.get("total_successful", 0)
    total_failed = download_totals.get("total_failed", 0)
    package_successful = download_totals.get("package_successful", 0)
    package_failed = download_totals.get("package_failed", 0)
    complete_successful = download_totals.get("complete_successful", 0)
    complete_failed = download_totals.get("complete_failed", 0)
    partial_successful = download_totals.get("partial_successful", 0)
    partial_failed = download_totals.get("partial_failed", 0)
    file_successful = download_totals.get("file_successful", 0)
    file_failed = download_totals.get("file_failed", 0)

    total_requests += 1
    if result == "SUCCESS":
        total_successful += 1
        if type == "FILE":
            file_successful += 1
        else:
            package_successful += 1
            if type == "COMPLETE":
                complete_successful += 1
            elif type == "PARTIAL":
                partial_successful += 1
    elif result == "FAILURE":
        total_failed += 1
        if type == "FILE":
            file_failed += 1
        else:
            package_failed += 1
            if type == "COMPLETE":
                complete_failed += 1
            elif type == "PARTIAL":
                partial_failed += 1

    download_totals["total_requests"] = total_requests 
    download_totals["total_successful"] = total_successful 
    download_totals["total_failed"] = total_failed 
    download_totals["package_successful"] = package_successful 
    download_totals["package_failed"] = package_failed 
    download_totals["complete_successful"] = complete_successful 
    download_totals["complete_failed"] = complete_failed 
    download_totals["partial_successful"] = partial_successful 
    download_totals["partial_failed"] = partial_failed 
    download_totals["file_successful"] = file_successful 
    download_totals["file_failed"] = file_failed 

    return download_totals


def buildSortedTotalsList(dataset_totals_dict):

    sorted_totals = []

    # Convert dictionary of objects to array of objects
    for dataset_id in dataset_totals_dict.keys():
        dataset_totals = dataset_totals_dict[dataset_id]
        dataset_totals["dataset_id"] = dataset_id
        sorted_totals.append(dataset_totals)

    # Sort totals by total_requests values, greatest to smallest
    if len(sorted_totals) > 0:
        sorted_totals.sort(key=operator.itemgetter("total_requests"), reverse=True)

    return sorted_totals


def calculateDownloadTotalsToDate(months, last_month):
    """
    Iterates over monthly totals up to and including the specified last month, and
    aggregates the totals into counts for all time, up to that last month.
    """

    updated_totals = {}

    # Aggregate monthly dataset totals as totals to date under dataset id

    for month in months:

        if month <= last_month:

            monthly_totals = totals_by_month[month]

            for dataset_id in monthly_totals.keys():

                monthly_dataset_totals = monthly_totals[dataset_id]
                updated_dataset_totals = updated_totals.get(dataset_id, {})

                for key in monthly_dataset_totals.keys():

                    monthly_total = monthly_dataset_totals[key]
                    current_total = updated_dataset_totals.get(key, 0)
                    updated_dataset_totals[key] = current_total + monthly_total

                updated_totals[dataset_id] = updated_dataset_totals
    
    return updated_totals


def calculateDownloadTotalsThisMonth(monthlyEvents):
    """
    Calculates the totals for a month given the monthly events
    """

    # Titles:
    # {id} / COMPLETE               / {result} 
    # {id} / PARTIAL  / {scope}     / {result} 
    # {id} / PACKAGE  / {filename}  / {result}
    # {id} / FILE     / {pathname}  / {result} 
    
    updated_totals = {}

    all = {
        "total_requests": 0,
        "total_successful": 0,
        "total_failed": 0,
        "package_successful": 0,
        "package_failed": 0,
        "complete_successful": 0,
        "complete_failed": 0,
        "partial_successful": 0,
        "partial_failed": 0,
        "file_successful": 0,
        "file_failed": 0
    }

    # Aggregate dataset monthly totals under dataset id
    for event_title in monthlyEvents:
        try:
            fields = event_title.split('/')
            dataset_id = fields[0].strip()
            event_type = fields[1].strip()
            if event_type == "COMPLETE":
                event_result = fields[2].strip()
            else:
                event_result = fields[3].strip()
        except Exception as error:
            print("Error: Malformed event title: %s" % event_title)
        dataset_totals = updated_totals.get(dataset_id, {})
        updated_totals[dataset_id] = updateDownloadTotals(dataset_totals, event_type, event_result)
        all = updateDownloadTotals(all, event_type, event_result)

    # Add totals for all datasets to dictionary
    updated_totals["ALL"] = all 

    return updated_totals


def outputTotalsToJsonFile(filename, totals):
    jsonFile = open("%s/%s" % (reports_root, filename), "w", encoding="utf-8")
    sortedTotals = buildSortedTotalsList(totals)
    first = True
    jsonFile.write('{\n')
    for dataset in sortedTotals:
        if not first:
            jsonFile.write(',\n')
        first = False
        jsonFile.write('    "%s": {\n' % dataset["dataset_id"])
        jsonFile.write('        "total_requests": %d,\n' % dataset["total_requests"])
        jsonFile.write('        "total_successful": %d,\n' % dataset["total_successful"])
        jsonFile.write('        "total_failed": %d,\n' % dataset["total_failed"])
        jsonFile.write('        "package_successful": %d,\n' % dataset["package_successful"])
        jsonFile.write('        "package_failed": %d,\n' % dataset["package_failed"])
        jsonFile.write('        "complete_successful": %d,\n' % dataset["complete_successful"])
        jsonFile.write('        "complete_failed": %d,\n' % dataset["complete_failed"])
        jsonFile.write('        "partial_successful": %d,\n' % dataset["partial_successful"])
        jsonFile.write('        "partial_failed": %d,\n' % dataset["partial_failed"])
        jsonFile.write('        "file_successful": %d,\n' % dataset["file_successful"])
        jsonFile.write('        "file_failed": %d\n' % dataset["file_failed"])
        jsonFile.write('    }')
    jsonFile.write('\n}\n')
    jsonFile.flush()
    jsonFile.close()


#--------------------------------------------------------------------------------

with open(events_file) as f:
    extracted_events = json.load(f)

for extracted_event in extracted_events:
    record_event_by_month(extracted_event)

months = list(events_by_month.keys())
months.sort()

for month in months:
    print(month)
    monthly_events = events_by_month[month]
    totals = calculateDownloadTotalsThisMonth(monthly_events)
    totals_by_month[month] = totals
    print("  month:   %s" % str(totals["ALL"]["total_requests"]))
    totals = calculateDownloadTotalsToDate(months, month)
    totals_to_date[month] = totals
    print("  to date: %s" % str(totals["ALL"]["total_requests"]))

for month in months:
    totals = totals_by_month[month]
    filename = "%s-TotalDownloadsPerMonth.json" % month
    print(filename)
    outputTotalsToJsonFile(filename, totals)
    totals = totals_to_date[month]
    filename = "%s-TotalDownloadsToDate.json" % month
    print(filename)
    outputTotalsToJsonFile(filename, totals)
