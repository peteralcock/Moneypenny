import csv
import logging
import re
from tldextract import tldextract
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Selector

class EmailItem(scrapy.Item):
    page_url = scrapy.Field()
    link = scrapy.Field()
    email = scrapy.Field()
    name = scrapy.Field()
    title = scrapy.Field()

class MITSpider(CrawlSpider):
    name = 'qs-contact-mit'
    allowed_domains = []
    file_CSV = open('companies.csv')
    data_CSV = csv.reader(file_CSV)
    list_CSV = list(data_CSV)
    start_urls = []

    for url in start_urls:
        home_domain = tldextract.extract(url).domain + '.' + tldextract.extract(url).suffix
        allowed_domains.append(home_domain)
    
    accept_keywords = ''.join([
        'relations|partners|invest|investor|manager|managers|merchants|vendors|retailers|sellers|dispensaries|clinics|shareholder'
    ])   
    accept_xpath = ''.join([
        '//a[contains(text(), "invest")',
        ' or contains(text(), "Invest")',
        ' or contains(text(), "relations")',
        ' or contains(text(), "Relations")',
        ' or contains(text(), "partners")',
        ' or contains(text(), "Partners")',
        ' or contains(text(), "sellers")', 
        ' or contains(text(), "Sellers")',
        ' or contains(text(), "merchants")', 
        ' or contains(text(), "Merchants")',
        ' or contains(text(), "vendors")', 
        ' or contains(text(), "Vendors")',
        ' or contains(text(), "managers")',
        ' or contains(text(), "Managers")',
        ' or contains(text(), "investors")',
        ' or contains(text(), "Investors")',
        ']'
    ])

    except_keywords_regex = ''.join([
        '^https?://([^/]+/){5,}[^/]+/?$' #match url at least 6 directories ex. home/dir1/dir2/dir3/dir4/dir5/dir6
        # '^https?://(\D*\d\D*){3,}$'
    ])

    except_keywords = ''.join([
        'youtube|google|mozilla|facebook|twitter|instagram|linkedin|pinterest'
        # '|([^/]+/){2,}[^/]+/?|java[Ss]cript'
    ])

    except_keywords_start = ''.join([
        '\?|#|java[Ss]cript|\.\.|tel\:|phone\:|fax\:'
    ])

    # common file extensions that are not followed if they occur in links
    IGNORED_EXTENSIONS = [
        # images
        'mng', 'pct', 'bmp', 'gif', 'jpg', 'jpeg', 'png', 'pst', 'psp', 'tif',
        'tiff', 'ai', 'drw', 'dxf', 'eps', 'ps', 'svg', 'webp',

        # audio
        'mp3', 'wma', 'ogg', 'wav', 'ra', 'aac', 'mid', 'au', 'aiff', 'webm',

        # video
        '3gp', 'asf', 'asx', 'avi', 'mov', 'mp4', 'mpg', 'mpeg', 'qt', 'rm', 'swf', 'wmv', 'flv',
        'm4a',

        # document
        'pdf', 'xls', 'xlsx', 'doc', 'docx', 'ppt', 'pps', 'pptx', 'pptm', 'ppsx', 'ppsm', 'sldx', 'sldm'
        'xps', 'rtf', 'odt', 'ods', 'odp', 'odg', 'odf',

        # other
        'css', 'exe', 'bin', 'rss', 'zip', 'rar', '7z', 'gz', 'bz2', 'tar'
    ]

    rules = (
        # Extract links matching 'category.php' (but not matching 'subsection.php')
        # and follow links from them (since no callback means follow=True by default).
        # Rule(LinkExtractor(allow=('', ), deny=('subsection\.php', ))),

        # Extract links matching 'item.php' and parse them with the spider's method parse_item
        Rule(
            LinkExtractor(
                allow=('^https?://[^\?#=]*('+accept_keywords+')[^\?#=]*$'), 
                # allow=(), 
                deny=('.*('+except_keywords+').*', '('+except_keywords_start+').*'), 
                # deny=(), 
                allow_domains=(allowed_domains), 
                deny_domains=(), 
                deny_extensions=IGNORED_EXTENSIONS, 
                restrict_xpaths=(), 
                restrict_css=(), 
                tags=('a', 'area'), 
                attrs=('href', ), 
                canonicalize=False, 
                unique=True, 
                process_value=None, 
                strip=True
            ), 
            callback='parse_item',
            follow=False
        ),
    )

    logger = logging.getLogger()
    logger.LOG_FILE = 'qs.log'
    logger.setLevel(logging.INFO)

    #Include the start url in the rule using a scrapy CrawlSpider
    #So override parse_start_url() and set callback to it, and then call parse_item()
    # def parse_start_url(self, response):
    #     self.logger.info('>>>>>>>> Parse start url: %s', response)
    #     # print 'response:'+str(response)
    #     return self.parse_item(response)

    def parse_item(self, response):
        # self.logger.info('allowed_domains: %s', self.allowed_domains)
        self.logger.info('>>>>>>>> Response page url: %s', response.url)
        if bool(re.match(self.except_keywords_regex, response.url)):
            self.logger.info('>>>>>>>> Ignoring response page url: %s', response.url)
            return

        sub_domain = tldextract.extract(response.url).subdomain
        domain = tldextract.extract(response.url).domain
        suffix = tldextract.extract(response.url).suffix
        page_root_domain = sub_domain + '.' + domain + '.' + suffix

        email_regex = r'^(mailto:)?[a-zA-Z0-9_.+-]+(@|[\[\(]?at[\]\)]?|[\[\(]?AT[\]\)]?)[a-zA-Z0-9-]+(\.|dot|DOT)[a-zA-Z0-9-.]+$'

        all_email_text = response.xpath('//body//*/text()').re(email_regex)
        email_count_in_text = len(all_email_text)
        all_email_link = response.xpath('//a/@href').re(email_regex)
        email_count_in_href = len(all_email_link)
#        yield {
#             'email_count_in_text': email_count_in_text,
#             'email_count_in_href': email_count_in_href
#         }
#        if email_count_in_text < 1 and email_count_in_href < 1:
#             self.logger.info('>>>>>>>> Ignoring response page url that contains no email: %s', response.url)
#             return
#        else:
        if email_count_in_text > 0 or email_count_in_href > 0:
            #Find logo in the page
            logo_type = ''
            logo_text = 'none'
            logo_element = response.xpath('//a')
            if logo_element.xpath('./text()').extract_first() is not None:
                logo_link = logo_element.xpath('./@href').extract_first()
                root_domain = re.sub(r'(http)s?://', '', logo_link)
                # is_logo = not bool(re.search(r'('+self.except_keywords_start+').*', logo_link))
                is_logo = False
                if page_root_domain == root_domain or logo_link == '/' or logo_link.endswith('home'):
                    is_logo = True
                if is_logo:
                    if len(logo_element.xpath('./text()').extract_first()) > 0:
                        logo_text = logo_element.xpath('./text()').extract_first()
                        # print 'logo text:'+logo_text
                        # logo_type = '_text'
                        # if not bool(re.match(r'^[\r\n\s]+$', logo_text)):
                        #     yield {
                        #     'logo_text': re.sub(r'[\r\n\s]+', ' ', logo_text)
                        # }
                        logo_text = re.sub(r'[\r\n\s]{2,}', ' ', logo_text)
                    elif len(logo_element.xpath('./@title').extract_first()) > 0:
                        logo_text = logo_element.xpath('./@title').extract_first()
                        # print 'logo title:'+logo_text
                        # logo_type = '_title'
                        # if not bool(re.match(r'^[\r\n\s]+$', logo_text)):
                        #     yield {
                        #     'logo_title': re.sub(r'[\r\n\s]+', ' ', logo_text)
                        # }
                        logo_text = re.sub(r'[\r\n\s]{2,}', ' ', logo_text)
            elif response.xpath('(//img)[1]').xpath('./text()').extract_first() is not None:
                logo_element = response.xpath('(//img)[1]')
                if len(logo_element.xpath('./@title').extract_first()) > 0:
                    logo_text = logo_element.xpath('./@title').extract_first()
                    # print 'logo img title:'+logo_text
                    # logo_type = '_img_title'
                    # if not bool(re.match(r'^[\r\n\s]+$', logo_text)):
                    #     yield {
                    #         'logo_img_title': re.sub(r'[\r\n\s]+', ' ', logo_text)
                    #     }
                    logo_text = re.sub(r'[\r\n\s]{2,}', ' ', logo_text)
                elif len(logo_element.xpath('./@alt').extract_first()) > 0:
                    logo_text = logo_element.xpath('./@alt').extract_first()
                    # print 'logo img alt:'+logo_text
                    # logo_type = '_img_alt'
                    # if not bool(re.match(r'^[\r\n\s]+$', logo_text)):
                    #     yield {
                    #         'logo_img_alt': re.sub(r'[\r\n\s]+', ' ', logo_text)
                    #     }
                    logo_text = re.sub(r'[\r\n\s]{2,}', ' ', logo_text)
            
            #Find header in the page
            header_text = ''
            header = response.xpath('(//header)[1]/*').extract_first()
            if header is not None and bool(re.match(header, r'^[a-zA-Z]+$')):
                header = re.sub(r'[\r\n\t]+', '', header)
                header = re.sub(r'\s+', ' ', header)
                header_text = header + ';'
            header_text_list = []
            header_element = response.xpath('//div[contains(@*, "Header") or contains(@*, "header")]')
            for header in header_element:
                header_sub_text = header.xpath('./text()').extract_first()
                if header_sub_text is not None and bool(re.match(header_sub_text, r'^[a-zA-Z]+$')):
                    header_sub_text = re.sub(r'[\r\n\t]+', '', header_sub_text)
                    header_sub_text = re.sub(r'\s+', ' ', header_sub_text)
                    header_text_list.append(header_sub_text + ';')
            header_text = header_text.join(header_text_list)

            #Find footer in the page
            footer_text = ''
            footer = response.xpath('(//footer)[1]/*').extract_first()
            if footer is not None and bool(re.match(footer, r'^[a-zA-Z]+$')):
                footer = re.sub(r'[\r\n\t]+', '', footer)
                footer = re.sub(r'\s+', ' ', footer)
                footer_text = footer + ';'
            footer_text_list = []
            footer_element = response.xpath('//div[contains(@*, "Footer") or contains(@*, "footer")]')
            for footer in footer_element:
                footer_sub_text = footer.xpath('./text()').extract_first()
                if footer_sub_text is not None and bool(re.match(footer_sub_text, r'^[a-zA-Z]+$')):
                    footer_sub_text = re.sub(r'\r\n', '', footer_sub_text)
                    footer_sub_text = re.sub(r'\s+', ' ', footer_sub_text)
                    footer_text_list.append(footer_sub_text + ';')
            footer_text = footer_text.join(footer_text_list)
            # yield {
            #     'footer_text': footer_text
            # }
            
            #Log response url meta
            yield {
                'page_url': response.url,
                'depth': response.meta['depth'],
                'sub_domain': sub_domain,
                'email_count_in_text': email_count_in_text,
                'email_count_in_href': email_count_in_href,
                'logo': logo_text,
                'header': header_text,
                'footer': footer_text
            }

            if email_count_in_href > 0:
                all_email_element = response.xpath('//a[starts-with(@href, "mailto:")]')
                # all_email_element = Selector(response=response).xpath('//a[re:test(@href, "'+email_regex+'")]')
                # print 'all_email_element:'+str(all_email_element)

                # all_email_element = response.xpath('//a[starts-with(@href, "mailto:")]')
                #XPath test regex : (//a[starts-with(@href, "mailto:")])[1]/../..//a[not(starts-with(@href, "mailto:"))]
                #XPath test regex : (//a[starts-with(@href, "mailto:")])[1]/ancestor-or-self::div[count(descendant::a) = 3]
                #XPath test regex : (//a[starts-with(@href, "mailto:")])[1]/ancestor-or-self::div[count(descendant::a) > 1 and count(descendant::a) < 5]
                #XPath test regex : (//a[starts-with(@href, "mailto:")])[1]/ancestor-or-self::*/h3
                for email in all_email_element:
                    link_info = link_info2 = link_info3 = link_info4 = ''
                    link_text = email.xpath('./text()').extract_first()
                    link_href = email.xpath('./@href').extract_first()
                    is_plain_email = bool(re.search(email_regex, link_href, flags=re.IGNORECASE))

                    if is_plain_email:
                        if link_text is not None:
                            if bool(re.search(email_regex, link_text, flags=re.IGNORECASE)):
                                # link_element = email.xpath('../../*[contains(@*, "title")]')
                                # info_elements = email.xpath('../..')
                                parent_element = email.xpath('name(..)').extract_first()
                                if (parent_element == 'td'):
                                    table_element = email.xpath('ancestor-or-self::table')
                                    table_head_element = table_element.xpath('./thead/tr/th/text()')
                                    table_head_text = ''
                                    for head in table_head_element:
                                        table_head_text = table_head_text + head + ';'
                                    table_data_element = email.xpath('ancestor-or-self::tr/td/text()')
                                    table_data_text = ''
                                    for data in table_data_element:
                                        table_data_text = table_data_text + data + ';'
                                    yield {
                                        'table_head_text': table_head_text,
                                        'table_data_text': table_data_text,
                                        'email': re.sub(r'mailto:', '', link_href, flags=re.IGNORECASE)
                                    }
                                else:    
                                    info_element = email.xpath('../..//a[not(starts-with(@href, "mailto:"))]')
                                    if info_element is not None:
                                        for info in info_element:
                                            info_text = info.xpath('./text()').extract_first()
                                            if info_text is not None and len(info_text) > 1:
                                                link_info = info_text
                                                break
                                    info_element = email.xpath('ancestor-or-self::*/h3')
                                    if info_element is not None:
                                        for info in info_element:
                                            info_text = info.xpath('./text()').extract_first()
                                            if info_text is not None and len(info_text) > 1:
                                                info_text = info_text + ';'
                                                link_info2 += info_text
                                    info_element = email.xpath('ancestor-or-self::*//h2')
                                    if info_element is not None:
                                        for info in info_element:
                                            info_text = info.xpath('./text()').extract_first()
                                            if info_text is not None and len(info_text) > 1:
                                                info_text = info_text + ';'
                                                link_info3 += info_text
                                    info_element = email.xpath('ancestor-or-self::*//h1')
                                    if info_element is not None:
                                        for info in info_element:
                                            info_text = info.xpath('./text()').extract_first()
                                            if info_text is not None and len(info_text) > 1:
                                                info_text = info_text + ';'
                                                link_info4 += info_text
                                    yield {
                                        'email': re.sub(r'mailto:', '', link_href, flags=re.IGNORECASE),
                                        'info': link_info,
                                        'info2': link_info2,
                                        'info3': link_info3,
                                        'info4': link_info4,
                                        'text': link_text
                                    }
                            else:
                                yield {
                                    'email': re.sub(r'mailto:', '', link_href, flags=re.IGNORECASE),
                                    'name': link_text,
                                    'text': link_text
                                }
                        else:
                            yield {
                                'email': re.sub(r'mailto:', '', link_href, flags=re.IGNORECASE),
                                'name': link_text,
                                'text': link_text
                            }
            elif email_count_in_text > 0:
                all_email_text_element = response.xpath('//body//*')
                for text_element in all_email_text_element:
                    text = text_element.xpath('./text()').extract_first()
                    if bool(re.match(email_regex, text, flags=re.IGNORECASE)):
                        info_element = text_element.xpath("ancestor-or-self::*[count(descendant::a) = 1]")
                        if info_element is not None:
                            for info in info_element:
                                info_text = info.xpath('//h2/text()').extract_first()
                                if info_text is not None and len(info_text) > 1:
                                    info_text = info_text + ';'
                                    link_info += info_text
                                info_text = info.xpath('//h3/text()').extract_first()
                                if info_text is not None and len(info_text) > 1:
                                    info_text = info_text + ';'
                                    link_info += info_text
                                info_text = info.xpath('//h4/text()').extract_first()
                                if info_text is not None and len(info_text) > 1:
                                    info_text = info_text + ';'
                                    link_info += info_text
                        yield {
                            'email': text,
                            'info': link_info
                        }
                        
