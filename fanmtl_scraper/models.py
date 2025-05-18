from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime

Base = declarative_base()


class Novel(Base):
    __tablename__ = 'novels'

    id = Column(Integer, primary_key=True)
    novel_id = Column(String(50), unique=True, nullable=False)
    title = Column(String(255), nullable=False)
    url = Column(String(255), nullable=False)
    chapters = Column(String(50))
    status = Column(String(50))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    chapters_rel = relationship("Chapter", back_populates="novel", cascade="all, delete-orphan")


class Chapter(Base):
    __tablename__ = 'chapters'

    id = Column(Integer, primary_key=True)
    novel_id = Column(String(50), ForeignKey('novels.novel_id'), nullable=False)
    chapter_number = Column(Integer, nullable=False)
    chapter_title = Column(String(255), nullable=False)
    chapter_url = Column(String(255), nullable=False)
    chapter_date = Column(String(50))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    novel = relationship("Novel", back_populates="chapters_rel")
    content = relationship("ChapterContent", back_populates="chapter", uselist=False, cascade="all, delete-orphan")


class ChapterContent(Base):
    __tablename__ = 'chapter_contents'

    id = Column(Integer, primary_key=True)
    chapter_id = Column(Integer, ForeignKey('chapters.id'), nullable=False)
    chapter_text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    chapter = relationship("Chapter", back_populates="content")


def create_tables(engine):
    Base.metadata.create_all(engine)