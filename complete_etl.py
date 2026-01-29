import pandas as pd
import logging
from typing import List, Dict, Optional, Set, Tuple
from datetime import datetime
from sqlalchemy import text
import re

from repositories.page_repository import PageRepository, Page
from repositories.base_repository import DatabaseConnection

logger = logging.getLogger(__name__)

class RepositoryBasedETL:
    """ETL using repository pattern"""
    
    def __init__(self, page_repo: PageRepository, db_url: str):
        self.page_repo = page_repo
        self.db_url = db_url
        self.db = DatabaseConnection(db_url)
        self.project_cache = {}  # cache for project name -> ID
        self.namespace_cache = {}  # cache for namespace name -> ID
    
    def _cache_projects_and_namespaces(self):
        """cache existing projects and namespaces"""
        with self.db.get_session() as session:
            # cache projects
            query = text("SELECT id, name FROM project")
            result = session.execute(query)
            for row in result:
                self.project_cache[row.name] = row.id
            
            # cache namespaces
            query = text("SELECT id, name FROM namespace")
            result = session.execute(query)
            for row in result:
                self.namespace_cache[row.name] = row.id
    
    def _get_or_create_cached(self, name: str, cache: Dict, table: str) -> Optional[int]:
        """get or create entity with caching"""
        if not name or pd.isna(name):
            return None
        
        # check cache 
        if name in cache:
            return cache[name]
        
        # create in database
        with self.db.get_session() as session:
            query = text(f"""
                INSERT INTO {table} (name) 
                VALUES (:name)
                RETURNING id
            """)
            result = session.execute(query, {'name': name})
            session.commit()
            
            entity_id = result.fetchone()[0]
            cache[name] = entity_id
            return entity_id
    
    def process_row_with_repository(self, row: Dict) -> bool:
        """process a single row"""
        try:
            title = str(row.get('title', '')).strip()
            if not title:
                return False
            
            # check if page exists
            existing = self.page_repo.get_by_title(title)
            if existing:
                return False
            
            # get or create project
            project_name = row.get('project_name')
            project_id = None
            if project_name and not pd.isna(project_name):
                project_id = self._get_or_create_cached(
                    str(project_name).strip(),
                    self.project_cache,
                    'project'
                )
            
            # get or create namespace
            namespace_name = row.get('namespace_name')
            namespace_id = None
            if namespace_name and not pd.isna(namespace_name):
                namespace_id = self._get_or_create_cached(
                    str(namespace_name).strip(),
                    self.namespace_cache,
                    'namespace'
                )
            
            # create page using repository
            page = Page(
                title=title,
                text=str(row.get('text', '')).strip(),
                views=int(row.get('view_count', 0) or 0),
                project_id=project_id,
                namespace_id=namespace_id,
                status='stub'
            )
            
            created = self.page_repo.create(page)
            if not created or not created.id:
                return False
            
            # process categories
            categories = row.get('categories')
            if categories and not pd.isna(categories):
                self._process_categories_for_page(created.id, str(categories))
            
            return True
            
        except Exception as e:
            logger.error(f"error processing row: {e}")
            return False
    
    def _process_categories_for_page(self, page_id: int, categories_str: str):
        """process categories using repository pattern"""
        from repositories.category_repository import CategoryRepository
        
        category_repo = CategoryRepository(self.db_url)
        categories = [cat.strip() for cat in categories_str.split(';') if cat.strip()]
        
        for category_name in categories:
            # get or create category
            category = category_repo.get_or_create_by_name(category_name)
            if category and category.id:
                # link page to category
                category_repo.link_page_to_category(page_id, category.id)

import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_simple_etl(csv_file: str, db_url: str):
    """
    simple one-function ETL
    """
    # setup database connection
    pageRepository = PageRepository(db_url)
    repositoryBasedETL = RepositoryBasedETL(pageRepository, db_url)
    logger.info(f"Reading CSV: {csv_file}")
    df = pd.read_csv(csv_file)
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    stats = {
        'total_rows': len(df),
        'pages_created': 0,
        'pages_skipped': 0
    }
    for idx, row in df.iterrows():
        result = repositoryBasedETL.process_row_with_repository(row.to_dict())
        if result == True:
            stats['pages_created'] += 1
        else:
            stats['pages_skipped'] += 1

    logger.info(f"finished. stats: {stats}")
    return(stats)