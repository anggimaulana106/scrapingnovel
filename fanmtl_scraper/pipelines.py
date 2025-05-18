import logging
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from .models import Novel, Chapter, ChapterContent, create_tables
from .items import NovelItem, ChapterItem, ChapterContentItem


class PostgreSQLPipeline:
    def __init__(self, postgres_uri):
        self.postgres_uri = postgres_uri
        self.engine = None
        self.Session = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            postgres_uri=crawler.settings.get('POSTGRES_URI'),
        )

    def open_spider(self, spider):
        try:
            self.engine = create_engine(self.postgres_uri)
            create_tables(self.engine)
            self.Session = sessionmaker(bind=self.engine)
            logging.info("PostgreSQL connection established successfully")
        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def close_spider(self, spider):
        if self.engine:
            self.engine.dispose()
            logging.info("PostgreSQL connection closed")

    def process_item(self, item, spider):
        session = self.Session()
        try:
            if isinstance(item, NovelItem):
                self._process_novel(item, session)
            elif isinstance(item, ChapterItem):
                self._process_chapter(item, session)
            elif isinstance(item, ChapterContentItem):
                self._process_chapter_content(item, session)
            
            session.commit()
            return item
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"Database error: {e}")
            raise
        except Exception as e:
            session.rollback()
            logging.error(f"Error processing item: {e}")
            raise
        finally:
            session.close()

    def _process_novel(self, item, session):
        novel = session.query(Novel).filter_by(novel_id=item['novel_id']).first()
        
        if novel:
            # Update existing novel
            novel.title = item['title']
            novel.url = item['url']
            novel.chapters = item['chapters']
            novel.status = item['status']
            novel.updated_at = datetime.datetime.utcnow()
        else:
            # Create new novel
            novel = Novel(
                novel_id=item['novel_id'],
                title=item['title'],
                url=item['url'],
                chapters=item['chapters'],
                status=item['status']
            )
            session.add(novel)

    def _process_chapter(self, item, session):
        chapter = session.query(Chapter).filter_by(
            novel_id=item['novel_id'],
            chapter_number=item['chapter_number']
        ).first()
        
        if chapter:
            # Update existing chapter
            chapter.chapter_title = item['chapter_title']
            chapter.chapter_url = item['chapter_url']
            chapter.chapter_date = item['chapter_date']
            chapter.updated_at = datetime.datetime.utcnow()
        else:
            # Create new chapter
            chapter = Chapter(
                novel_id=item['novel_id'],
                chapter_number=item['chapter_number'],
                chapter_title=item['chapter_title'],
                chapter_url=item['chapter_url'],
                chapter_date=item['chapter_date']
            )
            session.add(chapter)
            session.flush()  # Flush to get the ID
        
        return chapter

    def _process_chapter_content(self, item, session):
        chapter_content = session.query(ChapterContent).filter_by(
            chapter_id=item['chapter_id']
        ).first()
        
        if chapter_content:
            # Update existing content
            chapter_content.chapter_text = item['chapter_text']
            chapter_content.updated_at = datetime.datetime.utcnow()
        else:
            # Create new content
            chapter_content = ChapterContent(
                chapter_id=item['chapter_id'],
                chapter_text=item['chapter_text']
            )
            session.add(chapter_content)