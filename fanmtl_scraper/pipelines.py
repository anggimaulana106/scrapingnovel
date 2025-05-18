# In pipelines.py
import logging
import datetime
import traceback
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from .models import Novel, Chapter, ChapterContent, db_connect, create_table
from .items import NovelItem, ChapterItem, ChapterContentItem

class PostgreSQLPipeline:
    def __init__(self, db_url):
        self.db_url = db_url
        self.engine = None
        self.Session = None
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            db_url=crawler.settings.get('DATABASE_URL')
        )
    
    def open_spider(self, spider):
        # Create the engine when the spider opens
        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)
        # Ensure tables exist
        create_table(self.engine)
        spider.logger.info("PostgreSQLPipeline opened with engine: %s", self.engine)
    
    def close_spider(self, spider):
        spider.logger.info("PostgreSQLPipeline closed")
    
    def process_item(self, item, spider):
        session = self.Session()
        try:
            # Log the item being processed
            spider.logger.info(f"Processing item in pipeline: {type(item).__name__}")
            spider.logger.debug(f"Item data: {dict(item)}")
            
            if isinstance(item, NovelItem):
                spider.logger.debug("Item identified as NovelItem")
                self._process_novel(item, session, spider)
            elif isinstance(item, ChapterItem):
                spider.logger.debug("Item identified as ChapterItem")
                self._process_chapter(item, session, spider)
            elif isinstance(item, ChapterContentItem):
                spider.logger.debug("Item identified as ChapterContentItem")
                self._process_chapter_content(item, session, spider)
            else:
                spider.logger.warning(f"Unknown item type: {type(item).__name__}")
            
            session.commit()
            spider.logger.debug("Database session committed successfully")
            return item
        except SQLAlchemyError as e:
            session.rollback()
            spider.logger.error(f"Database error: {str(e)}")
            # Don't raise the exception, just log it and continue
            return item
        except Exception as e:
            session.rollback()
            spider.logger.error(f"Error processing item in pipeline: {str(e)}")
            spider.logger.error(f"Traceback: {traceback.format_exc()}")
            # Don't raise the exception, just log it and continue
            return item
        finally:
            session.close()
    
    def _process_novel(self, item, session, spider):
        try:
            # Check if the novel already exists
            novel = session.query(Novel).filter_by(novel_id=item['novel_id']).first()
            
            if novel:
                # Update existing novel
                for key, value in item.items():
                    if key != 'created_at':  # Don't update created_at
                        setattr(novel, key, value)
                novel.updated_at = datetime.datetime.utcnow()
                spider.logger.debug(f"Updated existing novel: {novel.title}")
            else:
                # Create new novel
                novel = Novel(**item)
                session.add(novel)
                spider.logger.debug(f"Added new novel: {novel.title}")
        
        except Exception as e:
            spider.logger.error(f"Error in _process_novel: {str(e)}")
            spider.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    def _process_chapter(self, item, session, spider):
        try:
            # Check if the novel exists
            novel = session.query(Novel).filter_by(novel_id=item['novel_id']).first()
            if not novel:
                spider.logger.warning(f"Novel with ID {item['novel_id']} not found, cannot add chapter")
                return
            
            # Check if the chapter already exists
            chapter = session.query(Chapter).filter_by(
                novel_id=item['novel_id'],
                chapter_number=item['chapter_number']
            ).first()
            
            if chapter:
                # Update existing chapter
                for key, value in item.items():
                    if key != 'created_at':  # Don't update created_at
                        setattr(chapter, key, value)
                chapter.updated_at = datetime.datetime.utcnow()
                spider.logger.debug(f"Updated existing chapter: {chapter.chapter_number}")
            else:
                # Create new chapter
                chapter = Chapter(**item)
                session.add(chapter)
                spider.logger.debug(f"Added new chapter: {chapter.chapter_number}")
            
        except Exception as e:
            spider.logger.error(f"Error in _process_chapter: {str(e)}")
            spider.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    def _process_chapter_content(self, item, session, spider):
        try:
            # Extract the chapter_id from the composite key
            chapter_id = item['chapter_id']
            
            # Parse the chapter_id to get novel_id and chapter_number
            parts = chapter_id.split('_')
            if len(parts) == 2:
                novel_id, chapter_number = parts
                try:
                    chapter_number = int(chapter_number)
                except ValueError:
                    spider.logger.warning(f"Invalid chapter number in chapter_id: {chapter_id}")
                    return
                
                # Find the chapter by novel_id and chapter_number
                chapter = session.query(Chapter).filter_by(
                    novel_id=novel_id,
                    chapter_number=chapter_number
                ).first()
                
                if not chapter:
                    spider.logger.warning(f"Chapter not found for novel_id={novel_id}, chapter_number={chapter_number}")
                    return
                
                # Use the chapter's ID for the chapter_content
                real_chapter_id = chapter.id
            else:
                # Try to use the chapter_id directly
                try:
                    real_chapter_id = int(chapter_id)
                except ValueError:
                    spider.logger.warning(f"Invalid chapter_id format: {chapter_id}")
                    return
            
            # Check if the chapter content already exists
            chapter_content = session.query(ChapterContent).filter_by(
                chapter_id=real_chapter_id
            ).first()
            
            if chapter_content:
                # Update existing chapter content
                for key, value in item.items():
                    if key != 'created_at' and key != 'chapter_id':  # Don't update created_at or chapter_id
                        setattr(chapter_content, key, value)
                chapter_content.updated_at = datetime.datetime.utcnow()
                spider.logger.debug(f"Updated existing chapter content for chapter_id: {real_chapter_id}")
            else:
                # Create new chapter content with the real chapter_id
                new_item = dict(item)
                new_item['chapter_id'] = real_chapter_id
                chapter_content = ChapterContent(**new_item)
                session.add(chapter_content)
                spider.logger.debug(f"Added new chapter content for chapter_id: {real_chapter_id}")
            
        except Exception as e:
            spider.logger.error(f"Error in _process_chapter_content: {str(e)}")
            spider.logger.error(f"Traceback: {traceback.format_exc()}")
            raise