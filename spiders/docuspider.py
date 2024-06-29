import asyncio
from twisted.internet import asyncioreactor

asyncioreactor.install(asyncio.get_event_loop())

import csv
import logging
from urllib.parse import urljoin
from tldextract import tldextract
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse

class DocumentItem(scrapy.Item):
    page_url = scrapy.Field()
    link_text = scrapy.Field()
    link_url = scrapy.Field()
class DocuSpider(CrawlSpider):
    name = 'document-spider'
    allowed_domains = []
    page_count = 0  # Counter for the number of pages crawled
    max_pages = 500  # Maximum number of pages to crawl

    # Load URLs from a CSV file
    with open('websites.csv') as file_CSV:
        data_CSV = csv.reader(file_CSV)
        start_urls = []
        for row in data_CSV:
            url = row[0]
            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url  # Add default scheme if missing
            start_urls.append(url)

    for url in start_urls:
        home_domain = tldextract.extract(url).domain + '.' + tldextract.extract(url).suffix
        allowed_domains.append(home_domain)

    rules = (
        Rule(
            LinkExtractor(
                allow_domains=allowed_domains,
                deny_domains=['facebook.com', 'instagram.com', 'tiktok.com', 'twitter.com','pinterest.com'],  # Ignore any Facebook domain or subdomain
                tags=('a', 'area'),
                attrs=('href',),
                canonicalize=False,
                unique=True
            ),
            callback='parse_item',
            follow=True
        ),
    )

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('spider.log')
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    def parse_item(self, response):
        self.logger.info('Processing page: %s', response.url)
        # Increment the page count and check the limit
        self.page_count += 1
        if self.page_count > self.max_pages:
            self.logger.info('Page limit reached. Stopping the spider.')
            self.crawler.engine.close_spider(self, 'Page limit reached')
            return

        # Only process HTML responses
        if not isinstance(response, HtmlResponse):
            self.logger.warning('Ignoring non-text response: %s', response.url)
            return
        links = response.xpath(
            '//a[contains(@href, ".pdf") or '
            'contains(@href, ".doc") or '
            'contains(@href, ".docx") or '
            'contains(@href, ".xls") or '
            'contains(@href, ".xlsx") or '
            'contains(@href, ".ppt") or '
            'contains(@href, ".pptx") or '
            'contains(@href, ".txt") or '
            'contains(@href, ".rtf") or '
            'contains(@href, ".odt") or '
            'contains(@href, ".ods") or '
            'contains(@href, ".odp")]'
        )


        for link in links:
            link_url = link.xpath('@href').get()
            if link_url and not link_url.startswith(('http://', 'https://')):
                link_url = urljoin(response.url, link_url)
            item = DocumentItem()
            item['page_url'] = response.url
            item['link_text'] = link.xpath('text()').get()
            item['link_url'] = link_url
            # Optionally download the file (assuming unique filteration of link URLs collected)
            # subprocess.run(["wget",  link])
            yield item
