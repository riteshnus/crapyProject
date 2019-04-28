import scrapy
from amazon.pipelines import KinesisPipelineNew
import uuid
import datetime
import time

class ReviewsSpider(scrapy.Spider):
	# Spider name
  name = 'amazon_re_reviews'
    
  myBaseUrl = "https://www.amazon.in/Apple-MacBook-Air-13-3-inch-MQD32HN/product-reviews/B073Q5R6VR/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews&pageNumber="
  start_urls=[]
  
  # Creating list of urls to be scraped by appending page number a the end of base url
  for i in range(1,2):
      start_urls.append(myBaseUrl+str(i))

  # Defining a Scrapy parser
  def parse(self, response):
    data = response.css('#cm_cr-review_list')
      
    # Collecting product star ratings
    star_rating = data.css('.review-rating')
      
    # Collecting user reviews
    comments = data.css('.review-text')
    count = 0
    
    # Mock pipeline
    # pipeline = KinesisPipelineNew()
    # print 'beforeMock'
    # print pipeline
    # pipeline.put_to_stream({'review': 'my name is anu', 'reviewId': '1'})

    # Combining the results
    # {'review': ''.join(review.xpath('.//text()').extract()),
    for review in star_rating:
        obj = {
                'reviewId': str(uuid.uuid4())[:8],
                'review': ''.join(comments[count].xpath(".//text()").extract()),
                'category': 'scapy',
                'timeStamp': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
              }
        yield obj
        pipeline = KinesisPipelineNew()
        pipeline.put_to_stream(obj)
        count=count+1
        
