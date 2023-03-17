import sys
from getopt import getopt
from aws import invoke_coordinator

args = sys.argv[1:]

file_path = args[0]
with open(file_path) as f:
    input_data = f.read()


invoke_coordinator(input_data)