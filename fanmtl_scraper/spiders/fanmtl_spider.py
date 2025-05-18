import scrapy
import re
import json
import logging
import math
import datetime
from urllib.parse import urljoin
from ..items import NovelItem, ChapterItem, ChapterContentItem


class FanmtlSpider(scrapy.Spider):
    name = 'fanmtl'
    allowed_domains = ['fanmtl.com']
    start_urls = ['https://www.fanmtl.com/list/all/all-onclick-1.html']
    
    custom_settings = {
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
    }
    
    def __init__(self, *args, **kwargs):
        super(FanmtlSpider, self).__init__(*args, **kwargs)
        self.novel_count = 0
        self.chapter_count = 0
        self.content_count = 0
        # Limit for testing purposes - set to None for full scrape
        self.max_novels = kwargs.get('max_novels', None)
        self.max_chapters_per_novel = kwargs.get('max_chapters_per_novel', None)
        
    def start_requests(self):
        """Override start_requests to log proxy and user agent information."""
        for url in self.start_urls:
            yield scrapy.Request(
                url=url, 
                callback=self.parse,
                meta={'log_stats': True}  # Flag to log request stats
            )
    
    def log_request_stats(self, response):
        """Log information about the request including proxy and user agent."""
        proxy = response.meta.get('_proxy_ip', 'None')
        user_agent = response.meta.get('_user_agent', 'Default')
        
        self.logger.info(
            f"Request stats for {response.url}:\n"
            f"Status: {response.status}\n"
            f"Proxy: {proxy}\n"
            f"User-Agent: {user_agent}\n"
            f"Response size: {len(response.body)} bytes"
        )
        
    def parse(self, response):
        """Parse the novel list page"""
        # Log request stats if needed
        if response.meta.get('log_stats', False):
            self.log_request_stats(response)
            
        # Check if we hit Cloudflare protection
        if "Cloudflare" in response.text and "security" in response.text:
            self.logger.warning(f"Cloudflare protection detected at {response.url}")
            yield scrapy.Request(
                response.url, 
                callback=self.parse, 
                dont_filter=True,
                meta={'log_stats': True}
            )
            return
            
        # Extract novel items
        novel_items = response.css('li.novel-item')
        
        if not novel_items:
            self.logger.warning(f"No novel items found on page: {response.url}")
            return
            
        for novel_item in novel_items:
            # Check if we've reached the maximum number of novels to scrape
            if self.max_novels and self.novel_count >= int(self.max_novels):
                self.logger.info(f"Reached maximum number of novels: {self.max_novels}")
                return
                
            title_element = novel_item.css('h4.novel-title.text2row')
            title = title_element.css('::text').get('').strip()
            
            relative_url = title_element.css('a::attr(href)').get('')
            url = urljoin('https://www.fanmtl.com', relative_url)
            
            # Extract novel_id from URL
            novel_id_match = re.search(r'/novel/([^.]+)\.html', relative_url)
            if not novel_id_match:
                self.logger.warning(f"Could not extract novel_id from URL: {relative_url}")
                continue
                
            novel_id = novel_id_match.group(1)
            
            # Extract chapters count and status
            chapters_text = novel_item.css('span.chapter::text').get('').strip()
            status = novel_item.css('span.status::text').get('').strip()
            
            novel = NovelItem(
                novel_id=novel_id,
                title=title,
                url=url,
                chapters=chapters_text,
                status=status,
                created_at=datetime.datetime.utcnow(),
                updated_at=datetime.datetime.utcnow()
            )
            
            self.novel_count += 1
            self.logger.info(f"Found novel: {title} ({novel_id})")
            
            yield novel
            
            # Request chapter list for this novel
            chapter_list_url = f'https://www.fanmtl.com/e/extend/fy.php?page=1&wjm={novel_id}'
            yield scrapy.Request(
                chapter_list_url,
                callback=self.parse_chapter_list,
                meta={
                    'novel_id': novel_id, 
                    'page': 1, 
                    'novel_title': title,
                    'log_stats': True
                }
            )
        
        # Follow pagination
        next_page = response.css('a.next::attr(href)').get()
        if next_page:
            next_page_url = urljoin('https://www.fanmtl.com', next_page)
            self.logger.info(f"Following next page: {next_page_url}")
            yield scrapy.Request(
                next_page_url, 
                callback=self.parse,
                meta={'log_stats': True}
            )
    
    def parse_chapter_list(self, response):
        """Parse the chapter list page"""
        # Log request stats if needed
        if response.meta.get('log_stats', False):
            self.log_request_stats(response)
            
        novel_id = response.meta['novel_id']
        current_page = response.meta['page']
        novel_title = response.meta['novel_title']
        
        try:
            # The response might be JSON
            data = json.loads(response.text)
            
            # Calculate total pages
            total_chapters = int(data.get('total', 0))
            items_per_page = 100  # Fanmtl typically shows 100 chapters per page
            total_pages = math.ceil(total_chapters / items_per_page)
            
            # Process chapters from JSON
            chapters = data.get('data', [])
            
            for idx, chapter in enumerate(chapters):
                # Check if we've reached the maximum number of chapters per novel
                if self.max_chapters_per_novel and idx >= int(self.max_chapters_per_novel):
                    self.logger.info(f"Reached maximum chapters per novel: {self.max_chapters_per_novel}")
                    break
                    
                chapter_number = chapter.get('id', 0)
                chapter_title = chapter.get('title', '').strip()
                relative_url = chapter.get('url', '')
                chapter_url = urljoin('https://www.fanmtl.com', relative_url)
                chapter_date = chapter.get('date', '')
                
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
                if self.chapter_count % 100 == 0:
                    self.logger.info(f"Processed {self.chapter_count} chapters so far")
                
                yield chapter_item
                
                # Request chapter content
                yield scrapy.Request(
                    chapter_url,
                    callback=self.parse_chapter_content,
                    meta={
                        'chapter_item': chapter_item,
                        'novel_title': novel_title,
                        'log_stats': True
                    }
                )
            
            # Request next page of chapters if available
            if current_page < total_pages:
                next_page = current_page + 1
                next_page_url = f'https://www.fanmtl.com/e/extend/fy.php?page={next_page}&wjm={novel_id}'
                
                yield scrapy.Request(
                    next_page_url,
                    callback=self.parse_chapter_list,
                    meta={
                        'novel_id': novel_id,
                        'page': next_page,
                        'novel_title': novel_title,
                        'log_stats': True
                    }
                )
                
        except json.JSONDecodeError:
            # If not JSON, try parsing HTML
            self.logger.warning(f"Failed to parse JSON from {response.url}, trying HTML parsing")
            
            chapter_items = response.css('ul.chapter-list li')
            
            for idx, chapter_item in enumerate(chapter_items):
                # Check if we've reached the maximum number of chapters per novel
                if self.max_chapters_per_novel and idx >= int(self.max_chapters_per_novel):
                    self.logger.info(f"Reached maximum chapters per novel: {self.max_chapters_per_novel}")
                    break
                    
                chapter_link = chapter_item.css('a')
                chapter_title = chapter_link.css('::text').get('').strip()
                relative_url = chapter_link.css('::attr(href)').get('')
                chapter_url = urljoin('https://www.fanmtl.com', relative_url)
                
                # Extract chapter number from title or position
                chapter_number_match = re.search(r'Chapter (\d+)', chapter_title)
                if chapter_number_match:
                    chapter_number = int(chapter_number_match.group(1))
                else:
                    # Use position as fallback
                    chapter_number = idx + 1
                
                chapter_date = chapter_item.css('span.time::text').get('').strip()
                
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
                if self.chapter_count % 100 == 0:
                    self.logger.info(f"Processed {self.chapter_count} chapters so far")
                
                yield chapter_item
                
                # Request chapter content
                yield scrapy.Request(
                    chapter_url,
                    callback=self.parse_chapter_content,
                    meta={
                        'chapter_item': chapter_item,
                        'novel_title': novel_title,
                        'log_stats': True
                    }
                )
            
            # Follow pagination if available
            next_page = response.css('a.next::attr(href)').get()
            if next_page:
                next_page_url = urljoin('https://www.fanmtl.com', next_page)
                
                yield scrapy.Request(
                    next_page_url,
                    callback=self.parse_chapter_list,
                    meta={
                        'novel_id': novel_id,
                        'page': current_page + 1,
                        'novel_title': novel_title,
                        'log_stats': True
                    }
                )
    
    def parse_chapter_content(self, response):
        """Parse the chapter content page"""
        # Log request stats if needed
        if response.meta.get('log_stats', False):
            self.log_request_stats(response)
            
        chapter_item = response.meta['chapter_item']
        novel_title = response.meta['novel_title']
        
        # Extract chapter content
        content_div = response.css('div.chapter-content')
        
        if not content_div:
            self.logger.warning(f"No content found for chapter: {chapter_item['chapter_url']}")
            return
        
        # Get all text nodes and paragraphs
        paragraphs = content_div.css('p::text').getall()
        text_nodes = content_div.css('::text').getall()
        
        # If paragraphs exist, use them; otherwise use all text nodes
        if paragraphs:
            chapter_text = '\n\n'.join([p.strip() for p in paragraphs if p.strip()])
        else:
            chapter_text = '\n\n'.join([t.strip() for t in text_nodes if t.strip()])
        
        # Create a placeholder for the chapter_id
        # In a real scenario, this would be the ID from the database
        # For now, we'll use a combination of novel_id and chapter_number
        chapter_id = f"{chapter_item['novel_id']}_{chapter_item['chapter_number']}"
        
        content_item = ChapterContentItem(
            chapter_id=chapter_id,
            chapter_text=chapter_text,
            created_at=datetime.datetime.utcnow(),
            updated_at=datetime.datetime.utcnow()
        )
        
        self.content_count += 1
        if self.content_count % 100 == 0:
            self.logger.info(f"Processed {self.content_count} chapter contents so far")
        
        yield content_item