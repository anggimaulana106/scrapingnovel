import scrapy

    # In items.py
class NovelItem(scrapy.Item):
    novel_id = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    chapters = scrapy.Field()
    status = scrapy.Field()
    cover_image_url = scrapy.Field()  # Add this field
    last_updated = scrapy.Field()     # Add this field
    created_at = scrapy.Field()
    updated_at = scrapy.Field()


class ChapterItem(scrapy.Item):
    novel_id = scrapy.Field()
    chapter_number = scrapy.Field()
    chapter_title = scrapy.Field()
    chapter_url = scrapy.Field()
    chapter_date = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()


class ChapterContentItem(scrapy.Item):
    chapter_id = scrapy.Field()
    chapter_text = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()