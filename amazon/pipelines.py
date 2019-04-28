import pymongo
from sqlalchemy.orm import sessionmaker
from amazon.models import engine, Category, CategoryUrl, Listing
from kinesis_producer import KinesisProducer
import boto3
import json
import random

my_stream_name = 'aws-scrapper'
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-1')

class PostgresPipeline(object):
	items = []
	batch_size = 100

	def open_spider(self, spider):
		self.session = sessionmaker(bind=engine)()

	def close_spider(self, spider):
		self.dump_into_db()
		self.session.close()

	def process_item(self, item, spider):
		model = self._process_item(item)
		self.items.append(model)

		if self.is_batch_completed():
			self.dump_into_db()

		return item

	def dump_into_db(self):
		self.session.add_all(self.items)
		self.session.commit()
		self.items = []

	def is_batch_completed(self):
		return len(self.items) > 100

class KinesisPipeline(object):
	
	def putIn(self, records):
		config = dict(
    aws_region='ap-southeast-1',
    buffer_size_limit=100000,
    buffer_time_limit=0.2,
    kinesis_concurrency=1,
    kinesis_max_retries=2,
    record_delimiter='\n',
    stream_name='aws-scrapper',
    )

		k = KinesisProducer(config=config)
	
		for record in records:
			k.send(record)

		k.close()
		k.join()

class KinesisPipelineNew():
	my_stream_name = 'aws-scrapper'
	kinesis_client = boto3.client('kinesis', region_name='ap-southeast-1')

	def put_to_stream(self, payload):
		print 'pre-payload'
		print payload

		put_response = kinesis_client.put_record(
												StreamName=my_stream_name,
												Data=json.dumps(payload),
												PartitionKey=str(random.randint(1,100)))
		print 'post-response'
		print put_response


class CategoriesPipeline(PostgresPipeline):
	def _process_item(self, item):
		category = Category(name=item['name'], image=item['image'])

		for url in item['urls']:
			category.urls.append(CategoryUrl(name=url['name'], url=url['url']))

		return category


class ListingPipeline(PostgresPipeline):
	def _process_item(self, item):
		return Listing(**dict(item))


class MongoPipeline(object):
	collection_name = 'listing'

	def __init__(self, mongo_uri, mongo_db):
		self.mongo_uri = mongo_uri
		self.mongo_db = mongo_db

	@classmethod
	def from_crawler(cls, crawler):
		return cls(
			mongo_uri=crawler.settings.get('MONGO_URI'),
			mongo_db=crawler.settings.get('MONGO_DATABASE', 'amazon')
		)

	def open_spider(self, spider):
		self.client = pymongo.MongoClient(self.mongo_uri)
		self.db = self.client[self.mongo_db]

	def close_spider(self, spider):
		self.client.close()

	def process_item(self, item, spider):
		self.db[self.collection_name].insert_one(dict(item))
		return item
