import os
import json
import sys
import operator

if not "FILE_1" in os.environ:
    print("Error: 'FILE_1' is not defined in environment", file=sys.stderr)
    os.abort()

if not "FILE_2" in os.environ:
    print("Error: 'FILE_2' is not defined in environment", file=sys.stderr)
    os.abort()

file1 = os.environ.get("FILE_1")
file2 = os.environ.get("FILE_2")

if not os.path.isfile(file1):
    print("Error: Could not find events file '%s'" % file1, file=sys.stderr)
    os.abort()

if not os.path.isfile(file2):
    print("Error: Could not find events file '%s'" % file2, file=sys.stderr)
    os.abort()

#print("  File 1: %s" % os.path.basename(file1))
#print("  File 2: %s" % os.path.basename(file2))

with open(file1) as file:
    file1_totals = json.load(file)

with open(file2) as file:
    file2_totals = json.load(file)

#--------------------------------------------------------------------------------

file1_dataset_ids = sorted(file1_totals.keys())
file2_dataset_ids = sorted(file2_totals.keys())

all_dataset_ids = sorted(file1_dataset_ids + file2_dataset_ids)

file1_dataset_count = len(file1_dataset_ids)
file2_dataset_count = len(file2_dataset_ids)

fields = [ "total_requests", "total_successful", "total_failed", "package_successful",
           "package_failed", "complete_successful", "complete_failed", "partial_successful",
           "partial_failed", "file_successful", "file_failed" ] 

#--------------------------------------------------------------------------------

def compareTotals(id):
    totals1 = file1_totals.get(id, {})
    totals2 = file2_totals.get(id, {})
    idOutput = False
    for field in fields:
        value1 = totals1.get(field, 0)
        value2 = totals2.get(field, 0)
        if value1 != value2:
            if not idOutput:
                print("    Values mismatch for dataset %s" % id)
                idOutput = True
            print("      Values for %s differ: %d %d" % ( field, value1, value2))

#--------------------------------------------------------------------------------

if file1_dataset_count != file2_dataset_count:
    print("    Dataset count mismatch: %d %d" % (file1_dataset_count, file2_dataset_count))
    for id in all_dataset_ids:
        if id not in file1_dataset_ids:
            print ("      Dataset missing from report 1: %s" % id)
        if id not in file2_dataset_ids:
            print ("      Dataset missing from report 2: %s" % id)

for id in all_dataset_ids:
    if id != "ALL":
        compareTotals(id)

compareTotals("ALL")
