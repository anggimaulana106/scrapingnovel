import scrapy
import re
import json
import logging
import math
import datetime
from urllib.parse import urljoin
import time
from scrapy.exceptions import CloseSpider
import traceback

from ..items import NovelItem, ChapterItem, ChapterContentItem


class FanmtlSpider(scrapy.Spider):
    """
    Spider for scraping novel data, chapter lists, and chapter contents from fanmtl.com
    
    This spider navigates through the novel list pages, extracts novel data,
    then for each novel, it extracts chapter lists and chapter contents using
    the specified URL formats and selectors.
    """
    name = 'fanmtl'
    allowed_domains = ['fanmtl.com']
    
    # Start with page 0 as per the URL pattern
    start_urls = ['https://www.fanmtl.com/list/all/all-onclick-0.html']
    
    # Custom settings for this spider
    custom_settings = {
        'DOWNLOAD_DELAY': 2.0,  # 2 second delay between requests
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,  # Limit concurrent requests
        'RETRY_TIMES': 5,  # Retry failed requests up to 5 times
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429, 403],  # HTTP codes to retry
        'LOG_LEVEL': 'DEBUG',  # Set log level to DEBUG for development
    }
    
    def __init__(self, *args, **kwargs):
        super(FanmtlSpider, self).__init__(*args, **kwargs)
        self.novel_count = 0
        self.chapter_count = 0
        self.content_count = 0
        self.page_count = 0
        
        # Optional limits for testing
        self.max_pages = kwargs.get('max_pages')
        self.max_novels = kwargs.get('max_novels')
        self.max_chapters_per_novel = kwargs.get('max_chapters_per_novel')
        
        # Convert string parameters to integers if provided
        if self.max_pages:
            self.max_pages = int(self.max_pages)
        if self.max_novels:
            self.max_novels = int(self.max_novels)
        if self.max_chapters_per_novel:
            self.max_chapters_per_novel = int(self.max_chapters_per_novel)
            
        self.logger.info(
            f"Spider initialized with max_pages={self.max_pages}, "
            f"max_novels={self.max_novels}, "
            f"max_chapters_per_novel={self.max_chapters_per_novel}"
        )
        
        # Set up file logging
        try:
            file_handler = logging.FileHandler('fanmtl_spider.log')
            file_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.info("File logging initialized")
        except Exception as e:
            self.logger.error(f"Failed to set up file logging: {str(e)}")
    
    def start_requests(self):
        """
        Start requests with custom headers and meta information
        to help with Cloudflare protection
        """
        for url in self.start_urls:
            self.logger.info(f"Starting request to {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    'page_number': 0,  # Track the current page number
                    'dont_redirect': True,
                    'handle_httpstatus_list': [403, 503, 404],  # Handle Cloudflare and not found status codes
                },
                headers=self._get_headers()
            )
    
    def parse(self, response):
        """
        Parse the novel list page, extract novel data, and handle pagination
        
        Args:
            response: The HTTP response object
            
        Yields:
            NovelItem: Novel data items
            Request: Requests for chapter lists and next page
        """
        try:
            # Check for Cloudflare protection
            if self._is_cloudflare_protected(response):
                for request in self._handle_cloudflare(response):
                    yield request
                return
            
            # Get current page number from meta
            current_page = response.meta.get('page_number', 0)
            self.page_count += 1
            
            self.logger.info(f"Processing page {current_page} (Page count: {self.page_count})")
            
            # Extract novel items
            novel_items = response.css('li.novel-item')
            
            if not novel_items:
                self.logger.warning(f"No novel items found on page: {response.url}")
                # Log the HTML for debugging
                self.logger.debug(f"Page HTML: {response.text[:1000]}...")
                # This could be the last page or an error
                return
            
            self.logger.info(f"Found {len(novel_items)} novels on page {current_page}")
            
            # Process each novel item
            for novel_item in novel_items:
                # Check if we've reached the maximum number of novels to scrape
                if self.max_novels and self.novel_count >= self.max_novels:
                    self.logger.info(f"Reached maximum number of novels: {self.max_novels}")
                    raise CloseSpider(f"Reached maximum number of novels: {self.max_novels}")
                
                try:
                    # Extract novel data
                    novel_data = self._extract_novel_data(novel_item, response)
                    
                    if novel_data:
                        self.novel_count += 1
                        self.logger.info(f"Extracted novel: {novel_data['title']} ({self.novel_count})")
                        self.logger.debug(f"Novel data: {dict(novel_data)}")
                        
                        # Yield the novel item
                        self.logger.info(f"Yielding NovelItem: {novel_data['title']}")
                        yield novel_data
                        
                        # Request the chapter list using the specified URL format
                        # Start with page 1 for chapter list
                        chapter_list_url = f"https://www.fanmtl.com/e/extend/fy.php?page=1&wjm={novel_data['novel_id']}"
                        
                        # Alternative URLs to try if the first one fails
                        alternative_urls = [
                            f"https://www.fanmtl.com/novel/{novel_data['novel_id']}.html",  # Direct novel page
                            f"https://www.fanmtl.com/novel/{novel_data['novel_id']}/index.html"  # Index page
                        ]
                        
                        self.logger.info(f"Requesting chapter list from: {chapter_list_url}")
                        
                        yield scrapy.Request(
                            url=chapter_list_url,
                            callback=self.parse_chapter_list,
                            meta={
                                'novel_id': novel_data['novel_id'],
                                'novel_title': novel_data['title'],
                                'total_chapters': novel_data['chapters'],
                                'page': 1,  # Start with page 1 for chapter list
                                'alternative_urls': alternative_urls,  # Pass alternative URLs
                                'dont_redirect': True,
                                'handle_httpstatus_list': [403, 503, 404],
                            },
                            headers=self._get_headers()
                        )
                
                except Exception as e:
                    self.logger.error(f"Error extracting novel data: {str(e)}")
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
                    # Continue with the next novel even if this one fails
                    continue
            
            # Check if we should proceed to the next page
            if self.max_pages and current_page >= self.max_pages - 1:
                self.logger.info(f"Reached maximum number of pages: {self.max_pages}")
                return
            
            # Follow pagination to the next page
            next_page_number = current_page + 1
            next_page_url = f'https://www.fanmtl.com/list/all/all-onclick-{next_page_number}.html'
            
            self.logger.info(f"Following next page: {next_page_url}")
            
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={
                    'page_number': next_page_number,
                    'dont_redirect': True,
                    'handle_httpstatus_list': [403, 503, 404],
                },
                headers=self._get_headers()
            )
        
        except Exception as e:
            self.logger.error(f"Error in parse method: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    def parse_chapter_list(self, response):
        """
        Parse the chapter list page to extract chapter information
        
        Args:
            response: The HTTP response object
            
        Yields:
            ChapterItem: Chapter data items
            Request: Requests for chapter content pages and next chapter list pages
        """
        try:
            # Check for Cloudflare protection
            if self._is_cloudflare_protected(response):
                for request in self._handle_cloudflare(response):
                    yield request
                return
            
            novel_id = response.meta['novel_id']
            novel_title = response.meta['novel_title']
            total_chapters = response.meta['total_chapters']
            current_page = response.meta['page']
            
            self.logger.info(f"Processing chapter list page {current_page} for novel: {novel_title} ({novel_id})")
            
            # Log the response for debugging
            self.logger.debug(f"Chapter list response for {novel_id}: {response.text[:200]}...")
            
            # Check if the response is empty or has an error status
            if response.status != 200 or not response.text:
                self.logger.warning(f"Invalid response for chapter list: status={response.status}, url={response.url}")
                
                # Try alternative URLs if available
                alternative_urls = response.meta.get('alternative_urls', [])
                if alternative_urls:
                    next_url = alternative_urls.pop(0)
                    self.logger.info(f"Trying alternative URL: {next_url}")
                    yield scrapy.Request(
                        url=next_url,
                        callback=self.parse_novel_detail,  # Use parse_novel_detail for HTML pages
                        meta={
                            'novel_id': novel_id,
                            'novel_title': novel_title,
                            'alternative_urls': alternative_urls,
                            'dont_redirect': True,
                            'handle_httpstatus_list': [403, 503, 404],
                        },
                        headers=self._get_headers()
                    )
                return
            
            try:
                # The response might be JSON
                data = json.loads(response.text)
                
                # Log the parsed JSON structure
                self.logger.debug(f"Parsed JSON structure: {list(data.keys()) if isinstance(data, dict) else 'Not a dictionary'}")
                
                # Calculate total pages
                total_pages = math.ceil(total_chapters / 100) if total_chapters > 0 else 1
                
                # Process chapters from JSON
                chapters = data.get('data', [])
                
                if not chapters:
                    self.logger.warning(f"No chapters found in JSON response for novel: {novel_title} ({novel_id})")
                    # Try alternative URLs or HTML parsing
                    raise json.JSONDecodeError("No chapters in JSON", response.text, 0)
                
                self.logger.info(f"Found {len(chapters)} chapters on page {current_page} for novel: {novel_title}")
                
                # Process each chapter
                for idx, chapter in enumerate(chapters):
                    # Check if we've reached the maximum number of chapters per novel
                    if self.max_chapters_per_novel and self.chapter_count >= self.max_chapters_per_novel:
                        self.logger.info(f"Reached maximum chapters per novel: {self.max_chapters_per_novel}")
                        break
                    
                    try:
                        # Extract chapter data from JSON
                        chapter_number = chapter.get('id', 0)
                        chapter_title = chapter.get('title', '').strip()
                        relative_url = chapter.get('url', '')
                        chapter_url = urljoin('https://www.fanmtl.com', relative_url)
                        chapter_date = chapter.get('date', '')
                        
                        # Validate chapter data
                        if not chapter_number or not chapter_title or not relative_url:
                            self.logger.warning(f"Missing required chapter data: number={chapter_number}, title={chapter_title}, url={relative_url}")
                            continue
                        
                        # Ensure chapter_number is an integer
                        if isinstance(chapter_number, str):
                            try:
                                chapter_number = int(chapter_number)
                            except ValueError:
                                self.logger.warning(f"Invalid chapter number: {chapter_number}, using index: {idx + 1}")
                                chapter_number = idx + 1
                        
                        # Create chapter item
                        chapter_item = ChapterItem(
                            novel_id=novel_id,
                            chapter_number=chapter_number,
                            chapter_title=chapter_title,
                            chapter_url=chapter_url,
                            chapter_date=chapter_date,
                            created_at=datetime.datetime.utcnow(),
                            updated_at=datetime.datetime.utcnow()
                        )
                        
                        self.chapter_count += 1
                        if self.chapter_count % 10 == 0:
                            self.logger.info(f"Processed {self.chapter_count} chapters so far")
                        
                        # Log the chapter item
                        self.logger.debug(f"Created chapter item: {dict(chapter_item)}")
                        
                        # Yield the chapter item
                        self.logger.info(f"Yielding ChapterItem: {chapter_title} (Chapter {chapter_number})")
                        yield chapter_item
                        
                        # Request the chapter content
                        # Use the specified URL format: https://www.fanmtl.com/novel/{novel_id}_{chapter_number}.html
                        content_url = f"https://www.fanmtl.com/novel/{novel_id}_{chapter_number}.html"
                        
                        # Alternative URLs to try
                        alternative_content_urls = [
                            f"https://www.fanmtl.com/read/{novel_id}/{chapter_number}.html",
                            chapter_url  # Use the URL extracted from the chapter list
                        ]
                        
                        self.logger.info(f"Requesting chapter content from: {content_url}")
                        
                        yield scrapy.Request(
                            url=content_url,
                            callback=self.parse_chapter_content,
                            meta={
                                'chapter_id': f"{novel_id}_{chapter_number}",  # Create a composite key
                                'novel_id': novel_id,
                                'chapter_number': chapter_number,
                                'chapter_title': chapter_title,
                                'alternative_urls': alternative_content_urls,
                                'dont_redirect': True,
                                'handle_httpstatus_list': [403, 503, 404],
                            },
                            headers=self._get_headers()
                        )
                    
                    except Exception as e:
                        self.logger.error(f"Error processing chapter data: {str(e)}")
                        self.logger.error(f"Traceback: {traceback.format_exc()}")
                        continue
                
                # Request next page of chapters if available
                if current_page < total_pages:
                    next_page = current_page + 1
                    next_page_url = f"https://www.fanmtl.com/e/extend/fy.php?page={next_page}&wjm={novel_id}"
                    
                    self.logger.info(f"Requesting next chapter list page: {next_page_url}")
                    
                    yield scrapy.Request(
                        url=next_page_url,
                        callback=self.parse_chapter_list,
                        meta={
                            'novel_id': novel_id,
                            'novel_title': novel_title,
                            'total_chapters': total_chapters,
                            'page': next_page,
                            'dont_redirect': True,
                            'handle_httpstatus_list': [403, 503, 404],
                        },
                        headers=self._get_headers()
                    )
            
            except json.JSONDecodeError as e:
                # If not JSON, try parsing HTML
                self.logger.warning(f"Failed to parse JSON from {response.url}: {str(e)}")
                self.logger.debug(f"Response content: {response.text[:500]}...")
                
                # Try alternative URLs if available
                alternative_urls = response.meta.get('alternative_urls', [])
                if alternative_urls:
                    next_url = alternative_urls.pop(0)
                    self.logger.info(f"Trying alternative URL: {next_url}")
                    yield scrapy.Request(
                        url=next_url,
                        callback=self.parse_novel_detail,  # Use parse_novel_detail for HTML pages
                        meta={
                            'novel_id': novel_id,
                            'novel_title': novel_title,
                            'alternative_urls': alternative_urls,
                            'dont_redirect': True,
                            'handle_httpstatus_list': [403, 503, 404],
                        },
                        headers=self._get_headers()
                    )
                    return
                
                # Extract chapter items from HTML
                chapter_items = response.css('ul.chapter-list li')
                
                if not chapter_items:
                    self.logger.warning(f"No chapter items found in HTML for novel: {novel_title} ({novel_id})")
                    return
                
                self.logger.info(f"Found {len(chapter_items)} chapters in HTML for novel: {novel_title}")
                
                # Process each chapter
                for idx, chapter_item in enumerate(chapter_items):
                    # Check if we've reached the maximum number of chapters per novel
                    if self.max_chapters_per_novel and self.chapter_count >= self.max_chapters_per_novel:
                        self.logger.info(f"Reached maximum chapters per novel: {self.max_chapters_per_novel}")
                        break
                    
                    try:
                        # Extract chapter data from HTML
                        chapter_link = chapter_item.css('a')
                        chapter_title = chapter_link.css('::text').get('').strip()
                        relative_url = chapter_link.css('::attr(href)').get('')
                        chapter_url = urljoin('https://www.fanmtl.com', relative_url)
                        
                        # Validate chapter data
                        if not chapter_title or not relative_url:
                            self.logger.warning(f"Missing required chapter data: title={chapter_title}, url={relative_url}")
                            continue
                        
                        # Extract chapter number from title or URL
                        chapter_number_match = re.search(r'Chapter (\d+)', chapter_title, re.IGNORECASE)
                        if chapter_number_match:
                            chapter_number = int(chapter_number_match.group(1))
                        else:
                            # Try to extract from URL
                            url_number_match = re.search(r'_(\d+)\.html$', relative_url)
                            if url_number_match:
                                chapter_number = int(url_number_match.group(1))
                            else:
                                # Use position as fallback
                                chapter_number = idx + 1
                        
                        # Extract chapter date
                        chapter_date = chapter_item.css('span.time::text').get('')
                        chapter_date = chapter_date.strip() if chapter_date else datetime.datetime.utcnow().strftime('%Y-%m-%d')
                        
                        # Create chapter item
                        chapter_item = ChapterItem(
                            novel_id=novel_id,
                            chapter_number=chapter_number,
                            chapter_title=chapter_title,
                            chapter_url=chapter_url,
                            chapter_date=chapter_date,
                            created_at=datetime.datetime.utcnow(),
                            updated_at=datetime.datetime.utcnow()
                        )
                        
                        self.chapter_count += 1
                        if self.chapter_count % 10 == 0:
                            self.logger.info(f"Processed {self.chapter_count} chapters so far")
                        
                        # Log the chapter item
                        self.logger.debug(f"Created chapter item from HTML: {dict(chapter_item)}")
                        
                        # Yield the chapter item
                        self.logger.info(f"Yielding ChapterItem from HTML: {chapter_title} (Chapter {chapter_number})")
                        yield chapter_item
                        
                        # Request the chapter content
                        # Use the specified URL format: https://www.fanmtl.com/novel/{novel_id}_{chapter_number}.html
                        content_url = f"https://www.fanmtl.com/novel/{novel_id}_{chapter_number}.html"
                        
                        # Alternative URLs to try
                        alternative_content_urls = [
                            f"https://www.fanmtl.com/read/{novel_id}/{chapter_number}.html",
                            chapter_url  # Use the URL extracted from the chapter list
                        ]
                        
                        self.logger.info(f"Requesting chapter content from: {content_url}")
                        
                        yield scrapy.Request(
                            url=content_url,
                            callback=self.parse_chapter_content,
                            meta={
                                'chapter_id': f"{novel_id}_{chapter_number}",  # Create a composite key
                                'novel_id': novel_id,
                                'chapter_number': chapter_number,
                                'chapter_title': chapter_title,
                                'alternative_urls': alternative_content_urls,
                                'dont_redirect': True,
                                'handle_httpstatus_list': [403, 503, 404],
                            },
                            headers=self._get_headers()
                        )
                    
                    except Exception as e:
                        self.logger.error(f"Error processing chapter data from HTML: {str(e)}")
                        self.logger.error(f"Traceback: {traceback.format_exc()}")
                        continue
        
        except Exception as e:
            self.logger.error(f"Error in parse_chapter_list method: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    def parse_novel_detail(self, response):
        """
        Parse the novel detail page to extract chapter list
        
        Args:
            response: The HTTP response object
            
        Yields:
            ChapterItem: Chapter data items
            Request: Requests for chapter content pages
        """
        try:
            # Check for Cloudflare protection
            if self._is_cloudflare_protected(response):
                for request in self._handle_cloudflare(response):
                    yield request
                return
            
            novel_id = response.meta['novel_id']
            novel_title = response.meta['novel_title']
            
            self.logger.info(f"Processing novel detail page for: {novel_title} ({novel_id})")
            
            # Log the HTML structure for debugging
            self.logger.debug(f"Novel detail HTML structure: {response.text[:500]}...")
            
            # Extract chapter list
            # First, try to find the chapter list container
            chapter_list = response.css('ul.chapter-list li')
            
            if not chapter_list:
                # If not found, try alternative selectors
                chapter_list = response.css('div.chapter-list a')
                
            if not chapter_list:
                self.logger.warning(f"No chapter list found for novel: {novel_title} ({novel_id})")
                # Try to find if there's a separate chapter list page
                chapter_list_url = response.css('a:contains("Chapter List")::attr(href)').get()
                if chapter_list_url:
                    chapter_list_url = urljoin(response.url, chapter_list_url)
                    self.logger.info(f"Found separate chapter list page: {chapter_list_url}")
                    yield scrapy.Request(
                        url=chapter_list_url,
                        callback=self.parse_chapter_list,
                        meta={
                            'novel_id': novel_id,
                            'novel_title': novel_title,
                            'total_chapters': 100,  # Default value
                            'page': 1,
                            'dont_redirect': True,
                            'handle_httpstatus_list': [403, 503, 404],
                        },
                        headers=self._get_headers()
                    )
                return
            
            self.logger.info(f"Found {len(chapter_list)} chapters for novel: {novel_title}")
            
            # Process each chapter
            for idx, chapter in enumerate(chapter_list):
                # Check if we've reached the maximum number of chapters per novel
                if self.max_chapters_per_novel and idx >= self.max_chapters_per_novel:
                    self.logger.info(f"Reached maximum chapters per novel: {self.max_chapters_per_novel}")
                    break
                    
                try:
                    # Extract chapter data
                    chapter_data = self._extract_chapter_data(chapter, novel_id, idx + 1, response)
                    
                    if chapter_data:
                        self.chapter_count += 1
                        if self.chapter_count % 10 == 0:
                            self.logger.info(f"Processed {self.chapter_count} chapters so far")
                        
                        # Log the chapter data
                        self.logger.debug(f"Extracted chapter data: {dict(chapter_data)}")
                        
                        # Yield the chapter item
                        self.logger.info(f"Yielding ChapterItem from novel detail: {chapter_data['chapter_title']} (Chapter {chapter_data['chapter_number']})")
                        yield chapter_data
                        
                        # Request the chapter content page
                        content_url = f"https://www.fanmtl.com/novel/{novel_id}_{chapter_data['chapter_number']}.html"
                        
                        # Alternative URLs to try
                        alternative_content_urls = [
                            f"https://www.fanmtl.com/read/{novel_id}/{chapter_data['chapter_number']}.html",
                            chapter_data['chapter_url']  # Use the URL extracted from the chapter list
                        ]
                        
                        self.logger.info(f"Requesting chapter content from: {content_url}")
                        
                        yield scrapy.Request(
                            url=content_url,
                            callback=self.parse_chapter_content,
                            meta={
                                'chapter_id': f"{novel_id}_{chapter_data['chapter_number']}",  # Create a composite key
                                'novel_id': novel_id,
                                'chapter_number': chapter_data['chapter_number'],
                                'chapter_title': chapter_data['chapter_title'],
                                'alternative_urls': alternative_content_urls,
                                'dont_redirect': True,
                                'handle_httpstatus_list': [403, 503, 404],
                            },
                            headers=self._get_headers()
                        )
                
                except Exception as e:
                    self.logger.error(f"Error extracting chapter data: {str(e)}")
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
                    # Continue with the next chapter even if this one fails
                    continue
        
        except Exception as e:
            self.logger.error(f"Error in parse_novel_detail method: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    def parse_chapter_content(self, response):
        """
        Parse the chapter content page to extract chapter text
        
        Args:
            response: The HTTP response object
            
        Yields:
            ChapterContentItem: Chapter content data item
        """
        try:
            # Check for Cloudflare protection
            if self._is_cloudflare_protected(response):
                for request in self._handle_cloudflare(response):
                    yield request
                return
            
            # Check if the response is valid
            if response.status != 200 or not response.text:
                self.logger.warning(f"Invalid response for chapter content: status={response.status}, url={response.url}")
                
                # Try alternative URLs if available
                alternative_urls = response.meta.get('alternative_urls', [])
                if alternative_urls:
                    next_url = alternative_urls.pop(0)
                    self.logger.info(f"Trying alternative URL for chapter content: {next_url}")
                    yield scrapy.Request(
                        url=next_url,
                        callback=self.parse_chapter_content,
                        meta={
                            'chapter_id': response.meta['chapter_id'],
                            'novel_id': response.meta['novel_id'],
                            'chapter_number': response.meta['chapter_number'],
                            'chapter_title': response.meta['chapter_title'],
                            'alternative_urls': alternative_urls,
                            'dont_redirect': True,
                            'handle_httpstatus_list': [403, 503, 404],
                        },
                        headers=self._get_headers()
                    )
                return
            
            chapter_id = response.meta['chapter_id']
            novel_id = response.meta['novel_id']
            chapter_number = response.meta['chapter_number']
            chapter_title = response.meta['chapter_title']
            
            self.logger.info(f"Processing chapter content: {chapter_title} (Chapter {chapter_number})")
            
            # Log the HTML structure for debugging
            self.logger.debug(f"Chapter content HTML structure: {response.text[:500]}...")
            
            try:
                # Try multiple selectors for content
                content_selectors = [
                    'div.chapter-content',
                    'div.content',
                    'article.content',
                    'div.text-content',
                    'div#content',
                    'div.novel-content'
                ]
                
                content_text = ""
                content_div = None
                
                # Try each selector until we find content
                for selector in content_selectors:
                    content_div = response.css(selector)
                    if content_div:
                        self.logger.debug(f"Found content using selector: {selector}")
                        break
                
                if not content_div:
                    self.logger.warning(f"No content div found for chapter: {chapter_title} (Chapter {chapter_number})")
                    # Log the full HTML for debugging
                    self.logger.debug(f"Full HTML: {response.text}")
                    return
                
                # Try different approaches to extract text
                # 1. Get paragraphs
                paragraphs = content_div.css('p::text').getall()
                if paragraphs and any(p.strip() for p in paragraphs):
                    content_text = '\n\n'.join([p.strip() for p in paragraphs if p.strip()])
                    self.logger.debug(f"Extracted {len(paragraphs)} paragraphs")
                else:
                    # 2. Get all text nodes
                    text_nodes = content_div.css('::text').getall()
                    if text_nodes and any(t.strip() for t in text_nodes):
                        content_text = '\n\n'.join([t.strip() for t in text_nodes if t.strip()])
                        self.logger.debug(f"Extracted {len(text_nodes)} text nodes")
                
                if not content_text:
                    self.logger.warning(f"No content text found for chapter: {chapter_title} (Chapter {chapter_number})")
                    return
                
                # Validate content length
                if len(content_text.strip()) < 10:  # Arbitrary minimum length
                    self.logger.warning(f"Chapter content too short for chapter: {chapter_title} (Chapter {chapter_number})")
                    return
                
                # Log a sample of the extracted content
                self.logger.debug(f"Extracted content sample: {content_text[:200]}...")
                
                # Create and yield the chapter content item
                chapter_content_item = ChapterContentItem(
                    chapter_id=chapter_id,
                    chapter_text=content_text,
                    created_at=datetime.datetime.utcnow(),
                    updated_at=datetime.datetime.utcnow()
                )
                
                self.content_count += 1
                if self.content_count % 10 == 0:
                    self.logger.info(f"Processed {self.content_count} chapter contents so far")
                
                # Log the chapter content item
                self.logger.debug(f"Created chapter content item for chapter_id: {chapter_id}")
                
                # Yield the chapter content item
                self.logger.info(f"Yielding ChapterContentItem for: {chapter_title} (Chapter {chapter_number})")
                yield chapter_content_item
                
            except Exception as e:
                self.logger.error(f"Error extracting chapter content: {str(e)}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        except Exception as e:
            self.logger.error(f"Error in parse_chapter_content method: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _extract_novel_data(self, novel_item, response):
        """
        Extract novel data from a novel item element
        
        Args:
            novel_item: The novel item HTML element
            response: The HTTP response object
            
        Returns:
            NovelItem: The extracted novel data
        """
        try:
            # Extract novel URL and title
            novel_url = novel_item.css('a::attr(href)').get('')
            title = novel_item.css('h4.novel-title.text2row::text').get('')
            
            # Clean the data
            novel_url = novel_url.strip() if novel_url else ''
            title = title.strip() if title else ''
            
            # Data validation
            if not novel_url or not title:
                self.logger.warning(f"Missing required novel data: URL={novel_url}, Title={title}")
                return None
            
            # Extract novel ID from URL
            novel_id_match = re.search(r'/novel/([^.]+)\.html', novel_url)
            if not novel_id_match:
                self.logger.warning(f"Could not extract novel_id from URL: {novel_url}")
                return None
                
            novel_id = novel_id_match.group(1)
            
            # Extract cover image URL - store this in a separate field if needed
            cover_image_rel_url = novel_item.css('figure.novel-cover img::attr(src)').get('')
            cover_image_url = urljoin('https://www.fanmtl.com', cover_image_rel_url) if cover_image_rel_url else ''
            
            # Extract chapter count - IMPORTANT: Extract just the number as an integer
            chapter_count_text = novel_item.css('div.novel-stats span:contains("Chapters")::text').get('')
            chapters = self._extract_number(chapter_count_text) if chapter_count_text else 0
            
            # Extract last updated - store this in a separate field if needed
            last_updated_text = novel_item.css('div.novel-stats span:contains("ago")::text').get('')
            last_updated = last_updated_text.strip() if last_updated_text else 'Unknown'
            
            # Extract status
            status = novel_item.css('div.novel-stats span.status::text').get('')
            status = status.strip() if status else 'Unknown'
            
            # Create and return the novel item
            # Using the field names from the original NovelItem class
            # IMPORTANT: chapters is now an integer, not a string
            return NovelItem(
                novel_id=novel_id,
                title=title,
                url=urljoin('https://www.fanmtl.com', novel_url),
                chapters=chapters,  # Integer value
                status=status,
                created_at=datetime.datetime.utcnow(),
                updated_at=datetime.datetime.utcnow()
            )
            
        except Exception as e:
            self.logger.error(f"Error in _extract_novel_data: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _extract_chapter_data(self, chapter_item, novel_id, position, response):
        """
        Extract chapter data from a chapter item element
        
        Args:
            chapter_item: The chapter item HTML element
            novel_id: The ID of the novel this chapter belongs to
            position: The position of this chapter in the list (fallback for chapter number)
            response: The HTTP response object
            
        Returns:
            ChapterItem: The extracted chapter data
        """
        try:
            # Extract chapter URL and title
            # Try different selectors depending on the HTML structure
            chapter_url = chapter_item.css('a::attr(href)').get('')
            if not chapter_url:
                chapter_url = chapter_item.css('::attr(href)').get('')
                
            chapter_title = chapter_item.css('a::text').get('')
            if not chapter_title:
                chapter_title = chapter_item.css('::text').get('')
            
            # Clean the data
            chapter_url = chapter_url.strip() if chapter_url else ''
            chapter_title = chapter_title.strip() if chapter_title else ''
            
            # Data validation
            if not chapter_url:
                self.logger.warning(f"Missing chapter URL for novel_id: {novel_id}, position: {position}")
                return None
            
            if not chapter_title:
                # Use a default title if none is found
                chapter_title = f"Chapter {position}"
            
            # Extract chapter number from title or URL
            chapter_number = position  # Default to position
            
            # Try to extract chapter number from title
            chapter_number_match = re.search(r'Chapter\s+(\d+)', chapter_title, re.IGNORECASE)
            if chapter_number_match:
                chapter_number = int(chapter_number_match.group(1))
            else:
                # Try to extract from URL
                url_number_match = re.search(r'/(\d+)\.html$', chapter_url)
                if url_number_match:
                    chapter_number = int(url_number_match.group(1))
            
            # Extract chapter date
            chapter_date = chapter_item.css('span.time::text, span.date::text').get('')
            chapter_date = chapter_date.strip() if chapter_date else None
            
            # If no date is found, use current date
            if not chapter_date:
                chapter_date = datetime.datetime.utcnow().strftime('%Y-%m-%d')
            
            # Create and return the chapter item
            return ChapterItem(
                novel_id=novel_id,
                chapter_number=chapter_number,
                chapter_title=chapter_title,
                chapter_url=urljoin(response.url, chapter_url),
                chapter_date=chapter_date,
                created_at=datetime.datetime.utcnow(),
                updated_at=datetime.datetime.utcnow()
            )
            
        except Exception as e:
            self.logger.error(f"Error in _extract_chapter_data: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _extract_number(self, text):
        """
        Extract a number from text
        
        Args:
            text: The text containing a number
            
        Returns:
            int: The extracted number, or 0 if no number is found
        """
        if not text:
            return 0
            
        # Find all numbers in the text
        numbers = re.findall(r'\d+', text)
        if numbers:
            return int(numbers[0])
        return 0
    
    def _is_cloudflare_protected(self, response):
        """
        Check if the response is protected by Cloudflare
        
        Args:
            response: The HTTP response object
            
        Returns:
            bool: True if protected by Cloudflare, False otherwise
        """
        # Check for Cloudflare protection indicators
        if response.status in [403, 503]:
            if "Cloudflare" in response.text or "security" in response.text.lower():
                return True
        return False
    
    def _handle_cloudflare(self, response):
        """
        Handle Cloudflare protection
        
        Args:
            response: The HTTP response object
            
        Yields:
            Request: A new request with a delay to retry
        """
        self.logger.warning(f"Cloudflare protection detected at {response.url}")
        
        # Add a delay before retrying
        time.sleep(5)  # 5 second delay
        
        # Retry the request with a different User-Agent
        yield scrapy.Request(
            url=response.url,
            callback=response.request.callback,
            dont_filter=True,  # Don't filter duplicate requests
            meta=response.meta,
            headers=self._get_alternate_headers()
        )
    
    def _get_headers(self):
        """
        Get headers for HTTP requests
        
        Returns:
            dict: Headers for HTTP requests
        """
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.fanmtl.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
    
    def _get_alternate_headers(self):
        """
        Get alternate headers for HTTP requests when dealing with Cloudflare
        
        Returns:
            dict: Alternate headers for HTTP requests
        """
        return {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.fanmtl.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
    
    def _clean_chapter_text(self, text):
        """
        Clean chapter text
        
        Args:
            text: The chapter text to clean
            
        Returns:
            str: The cleaned chapter text
        """
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove common ads or unwanted text
        text = re.sub(r'If you find any errors $$ broken links, non-standard content, etc\.\.$$, Please let us know.*', '', text, flags=re.IGNORECASE)
        
        # Split into paragraphs and clean each paragraph
        paragraphs = text.split('\n\n')
        cleaned_paragraphs = [p.strip() for p in paragraphs if p.strip()]
        
        return '\n\n'.join(cleaned_paragraphs)