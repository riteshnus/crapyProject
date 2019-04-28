import scrapy
from amazon.pipelines import KinesisPipelineNew
import uuid
import webhoseio
import time
import datetime


class WebhoseSpider(scrapy.Spider):
	# Spider name
  name = 'amazon_api_webhose'
    
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
    pipeline = KinesisPipelineNew()
    pipeline.put_to_stream(obj)
        
