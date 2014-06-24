#!/usr/bin/python

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import sys
import json
import urllib
import md5
import boto
import time
import config

# Vanity
class bcolours:
	HEADER = "\033[95m"
	OKBLUE = "\033[94m"
	OKGREEN = "\033[92m"
	WARNING = "\033[93m"
	FAIL = "\033[91m"
	ENDC = "\033[0m"

stream_name = raw_input("Please enter " + bcolours.OKBLUE + "stream name" + bcolours.ENDC + " to consume: ")

kinesis = boto.connect_kinesis(aws_access_key_id=config.AWS_ACCESS_KEY_ID, aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)

# Wait for the stream to be ready
tries = 0
while tries < 10:
	tries += 1
	time.sleep(15)
	response = kinesis.describe_stream(stream_name)
	if response["StreamDescription"]["StreamStatus"] == "ACTIVE":
		shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]
		break
else:
	raise TimeoutError("Stream is still not active, " + bcolours.FAIL + "aborting" + bcolours.ENDC + "...")

print "Kinesis stream is " + bcolours.OKGREEN + "active" + bcolours.ENDC + ", starting consumer..."

response = kinesis.get_shard_iterator(stream_name, shard_id, "TRIM_HORIZON")
shard_iterator = response["ShardIterator"]

while True:
	time.sleep(1)

	response = kinesis.get_records(shard_iterator)
	shard_iterator = response["NextShardIterator"]

	if len(response["Records"]):
		for record in response["Records"]:
			data = json.loads(record["Data"])
			print bcolours.OKGREEN + "Received" + bcolours.ENDC + ": " + data["title"]
	else:
		print bcolours.OKBLUE + "No records" + bcolours.ENDC + ", continuing"