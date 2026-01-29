# repositories/base_repository.py
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, TypeVar, Generic
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from dataclasses import dataclass
from datetime import datetime
import json

logger = logging.getLogger(__name__)

T = TypeVar('T')

@dataclass
class Page:
    """data class representing a page entity"""
    id: Optional[int] = None
    title: str = ""
    project_id: Optional[int] = None
    views: int = 0
    status: str = "stub"
    namespace_id: Optional[int] = None
    text: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Category:
    """data class representing a category entity"""
    id: Optional[int] = None
    name: str = ""
    text_content: str = ""
    status: str = "stub"

@dataclass
class Page_Category:
    """data class representing a page and category relation (page belongs to category)"""
    page_id: int
    category_id: int

class DatabaseConnection:
    """manage database connections using connection pooling"""
    _instances = {}
    
    def __new__(cls, db_url: str):
        if db_url not in cls._instances:
            instance = super().__new__(cls)
            instance.engine = create_engine(
                db_url,
                pool_size=10,
                max_overflow=20,
                pool_recycle=3600,
                echo=False
            )
            instance.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=instance.engine
            )
            cls._instances[db_url] = instance
        return cls._instances[db_url]
    
    @contextmanager
    def get_session(self):
        """context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"session rollback due to error: {e}")
            raise
        finally:
            session.close()

class BaseRepository(ABC, Generic[T]):
    """abstract base repository with common operations"""
    
    def __init__(self, db_url: str):
        self.db = DatabaseConnection(db_url)
    
    @abstractmethod
    def _to_entity(self, row: Dict) -> T:
        """convert database row to entity"""
        pass
    
    @abstractmethod
    def _from_entity(self, entity: T) -> Dict:
        """convert entity to database row"""
        pass
    
    def _execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """execute a raw SQL query and return results"""
        with self.db.get_session() as session:
            result = session.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]
    
    def _execute_update(self, query: str, params: Dict = None) -> int:
        """execute an update/insert/delete query"""
        with self.db.get_session() as session:
            result = session.execute(text(query), params or {})
            return result.rowcount