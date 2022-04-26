# import the spiders you want to run
from spiders.toscrape import ToScrapeSpider
from spiders.toscrape2 import ToScrapeSpiderTwo

# scrapy api imports
# from scrapy import signals, log
from scrapy import signals
import logging
from twisted.internet import reactor
# from scrapy.crawler import Crawler
from scrapy.crawler import CrawlerProcess
# from scrapy.crawler import CrawlerRunner
from scrapy.settings import Settings

process = CrawlerProcess()
process.crawl(ToScrapeSpider)
process.crawl(ToScrapeSpiderTwo)
process.start() # the script will block here until all crawling jobs are finished