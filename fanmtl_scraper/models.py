# In models.py
from sqlalchemy import create_engine, Column, Table, ForeignKey, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, Date, DateTime, Text, Boolean
from sqlalchemy.orm import relationship
import datetime

Base = declarative_base()

def db_connect():
    """
    Creates database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine("postgresql://postgres:salonpas@localhost:5444/postgres")
    

def create_table(engine):
    """
    Create the tables in the database
    """
    Base.metadata.create_all(engine)

class Novel(Base):
    __tablename__ = "novels"

    id = Column(Integer, primary_key=True)
    novel_id = Column(String(255), unique=True)
    title = Column(String(255))
    url = Column(String(255))
    chapters = Column(Integer)
    status = Column(String(50))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

    # Relationship with chapters
    chapters_rel = relationship("Chapter", back_populates="novel")

class Chapter(Base):
    __tablename__ = "chapters"

    id = Column(Integer, primary_key=True)
    novel_id = Column(String(255), ForeignKey("novels.novel_id"))
    chapter_number = Column(Integer)
    chapter_title = Column(String(255))
    chapter_url = Column(String(255))
    chapter_date = Column(String(50))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

    # Relationship with novel
    novel = relationship("Novel", back_populates="chapters_rel")
    # Relationship with chapter content
    content = relationship("ChapterContent", back_populates="chapter", uselist=False)

class ChapterContent(Base):
    __tablename__ = "chapter_contents"

    id = Column(Integer, primary_key=True)
    chapter_id = Column(Integer, ForeignKey("chapters.id"))
    chapter_text = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

    # Relationship with chapter
    chapter = relationship("Chapter", back_populates="content")