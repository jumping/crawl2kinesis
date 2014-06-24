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

port = 8888
if len(sys.argv) > 1:
	port = int(sys.argv[1])

kinesis = False
stream_name = "cmdlinecrawl_%i" % time.time()

class ImportIoCrawlerHandler(BaseHTTPRequestHandler):

	def do_POST(self):
		global kinesis
		global stream_name
		length = int(self.headers["content-length"])
		data = self.rfile.read(length)
		try:
			json_data = json.loads(data)
			if len(json_data["results"]) < 1:
				self.send_response(200)
				self.end_headers()
				return
			for position, index_data in enumerate(json_data["results"]):
				send_data = json.dumps(index_data)
				send_hash = md5.new(send_data).hexdigest()
				kinesis.put_record(stream_name, send_data, send_hash)
				print bcolours.OKGREEN + "Queued" + bcolours.ENDC + ": " + index_data["title"]
			self.send_response(200);
			self.end_headers()
		except Exception as e:
			print bcolours.FAIL + "Unable to process" + bcolours.ENDC + ": %s; %s" % (data, e)
			self.send_response(500)
			self.end_headers()

	# Silencio!
	def log_message(self, format, *args):
		return

def main():
	global kinesis
	global stream_name

	print "Starting Kinesis, " + bcolours.WARNING + "please wait before starting the crawler" + bcolours.ENDC + "!"
	kinesis = boto.connect_kinesis(aws_access_key_id=config.AWS_ACCESS_KEY_ID, aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)

	# Create steam
	kinesis.create_stream(stream_name, 1)

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

	print "Kinesis stream is " + bcolours.OKGREEN + "active" + bcolours.ENDC + ", starting server..."

	print "Kinesis stream name: " + bcolours.HEADER + stream_name + bcolours.ENDC

	try:
		server = HTTPServer(("", port), ImportIoCrawlerHandler)
		print "Server started on port " + str(port) + ", " + bcolours.OKGREEN + "ready to recieve crawl data" + bcolours.ENDC
		server.serve_forever()
	except KeyboardInterrupt:
		print "Server " + bcolours.FAIL + "shutting down" + bcolours.ENDC
		server.socket.close()
		print bcolours.FAIL + "Deleting" + bcolours.ENDC + " Kinesis stream"
		kinesis.delete_stream(stream_name)
		print bcolours.OKGREEN + "Done" + bcolours.ENDC

if __name__ == "__main__":
	main()
