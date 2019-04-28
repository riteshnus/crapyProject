import uuid
import webhoseio
import time
import datetime
import boto3
import json
import random

my_stream_name = 'aws-scrapper'
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-1')

def lambda_handler(event, context):
    schedule()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def put_to_stream(payload):
		print 'pre-payload'
		print payload

		put_response = kinesis_client.put_record(
												StreamName=my_stream_name,
												Data=json.dumps(payload),
												PartitionKey=str(random.randint(1,100)))
		print 'post-response'
		print put_response

def schedule():
  webhoseio.config(token="85f4ada8-2a5a-4cbf-9fb8-d2524a403b3c")
  s = int(time.time()) - 500
  query_params = {"q": "language:english site:amazon.com site_category:shopping", "ts": "{}".format(s), "sort": "crawled"}
  output = webhoseio.query("filterWebContent", query_params)
  print len(output)
  # for i in range(0,len(output)):
  #   review_output = output['posts'][i]['text']
  #   # print 'before-output'
  #   print review_output
  
  for i in range(len(output)):
    obj = {
            'reviewId': str(uuid.uuid4())[:8],
            'review': output['posts'][i]['text'],
            'category': 'webhose',
            'productname': output['posts'][i]['thread']['title'],
            'reviewername': output['posts'][i]['author'],
            'reviewdate': output['posts'][i]['thread']['published'],
            'rating': output['posts'][i]['rating'],
            'timeStamp': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
          }
    print obj
    put_to_stream(obj)