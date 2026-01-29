from decimal import Decimal
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import json
from .base_repository import BaseRepository, Page, DatabaseConnection
from sqlalchemy.exc import SQLAlchemyError
import logging
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PageRepository(BaseRepository[Page]):
    """repository for page table operations"""
    
    def __init__(self, db_url: str):
        super().__init__(db_url)
        self.table_name = "page"
    
    def _to_entity(self, row: Dict) -> Page:
        """convert database row to page entity"""
        return Page(
            id=row.get('id'),
            title=row.get('title', ''),
            project_id=row.get('project_id'),
            views=row.get('views', 0),
            status=row.get('status', 'active'),
            namespace_id=row.get('namespace_id'),
            text=row.get('text', '')
        )
    
    def _from_entity(self, page: Page) -> Dict:
        """convert page entity to database row"""
        data = {
            'title': page.title,
            'project_id': page.project_id,
            'views': page.views,
            'status': page.status,
            'namespace_id': page.namespace_id,
            'text': page.text
        }
        
        if page.id:
            data['id'] = page.id
            
        return {k: v for k, v in data.items() if v is not None}
    
    # CREATE Operations
    
    def create(self, page: Page) -> Optional[Page]:
        """create a new page"""
        try:
            # Check if page with same title exists
            if self.get_by_title(page.title):
                logger.warning(f"page with title '{page.title}' already exists")
                return None
            
            data = self._from_entity(page)
            
            columns = ', '.join(data.keys())
            placeholders = ', '.join([f':{key}' for key in data.keys()])
            returning_clause = "RETURNING id"
            
            query = f"""
                INSERT INTO {self.table_name} ({columns})
                VALUES ({placeholders})
                {returning_clause}
            """
            
            with self.db.get_session() as session:
                result = session.execute(text(query), data)
                session.commit()
                
                if result:
                    row = result.fetchone()
                    if row:
                        page.id = row.id
                        logger.info(f"created page with ID: {page.id}")
                        return page
            
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"failed to create page: {e}")
            return None
    
    def create_batch(self, pages: List[Page]) -> List[Page]:
        """create multiple pages at once"""
        created_pages = []
        for page in pages:
            created = self.create(page)
            if created:
                created_pages.append(created)
        return created_pages
    
    # READ Operations
    
    def get_by_id(self, page_id: int) -> Optional[Page]:
        """get page by ID"""
        try:
            query = f"""
                SELECT p.*, 
                       pr.name as project_name,
                       n.name as namespace_name
                FROM {self.table_name} p
                LEFT JOIN project pr ON p.project_id = pr.id
                LEFT JOIN namespace n ON p.namespace_id = n.id
                WHERE p.id = :page_id
            """
            
            result = self._execute_query(query, {'page_id': page_id})
            if result:
                return self._to_entity(result[0])
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"failed to get page by ID {page_id}: {e}")
            return None
    
    def get_by_title(self, title: str) -> Optional[Page]:
        """get page by title (case-insensitive)"""
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                WHERE LOWER(title) = LOWER(:title)
                LIMIT 1
            """
            
            result = self._execute_query(query, {'title': title})
            if result:
                return self._to_entity(result[0])
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"failed to get page by title '{title}': {e}")
            return None
    
    def get_all(self, limit: int = 100, offset: int = 0) -> List[Page]:
        """get all pages with pagination"""
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                ORDER BY id 
                LIMIT :limit OFFSET :offset
            """
            
            result = self._execute_query(query, {'limit': limit, 'offset': offset})
            return [self._to_entity(row) for row in result]
            
        except SQLAlchemyError as e:
            logger.error(f"failed to get all pages: {e}")
            return []
    
    def search(self, keyword: str, limit: int = 50) -> List[Page]:
        """search pages by keyword in title or text"""
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                WHERE title ILIKE :keyword 
                   OR text ILIKE :keyword
                ORDER BY 
                    CASE 
                        WHEN title ILIKE :keyword_exact THEN 1
                        WHEN text ILIKE :keyword_exact THEN 2
                        ELSE 3
                    END,
                    views DESC
                LIMIT :limit
            """
            
            params = {
                'keyword': f'%{keyword}%',
                'keyword_exact': f'%{keyword}%',
                'limit': limit
            }
            
            result = self._execute_query(query, params)
            return [self._to_entity(row) for row in result]
            
        except SQLAlchemyError as e:
            logger.error(f"failed to search pages with keyword '{keyword}': {e}")
            return []
    
    def get_by_project(self, project_id: int, limit: int = 100) -> List[Page]:
        """get pages by project ID"""
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                WHERE project_id = :project_id
                ORDER BY views DESC
                LIMIT :limit
            """
            
            result = self._execute_query(query, {'project_id': project_id, 'limit': limit})
            return [self._to_entity(row) for row in result]
            
        except SQLAlchemyError as e:
            logger.error(f"failed to get pages for project {project_id}: {e}")
            return []
    
    def get_top_viewed(self, limit: int = 10) -> List[Page]:
        """get top viewed pages"""
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                ORDER BY views DESC 
                LIMIT :limit
            """
            
            result = self._execute_query(query, {'limit': limit})
            return [self._to_entity(row) for row in result]
            
        except SQLAlchemyError as e:
            logger.error(f"failed to get top viewed pages: {e}")
            return []
    
    # UPDATE Operations
    
    def update(self, page: Page) -> bool:
        """update an existing page"""
        try:
            if not page.id:
                logger.error("cannot update page without ID")
                return False
            
            data = self._from_entity(page)
            
            set_clause = ', '.join([f"{key} = :{key}" for key in data.keys()])
            
            query = f"""
                UPDATE {self.table_name} 
                SET {set_clause}
                WHERE id = :id
            """
            
            #data['id'] = page.id
            data['id'] = 'DEFAULT'
            rowcount = self._execute_update(query, data)
            
            if rowcount > 0:
                logger.info(f"updated page with ID: {page.id}")
                return True
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to update page {page.id}: {e}")
            return False
    
    def update_views(self, page_id: int, increment: int = 1) -> bool:
        """increment page views"""
        try:
            query = f"""
                UPDATE {self.table_name} 
                SET views = views + :increment
                WHERE id = :page_id
            """
            
            rowcount = self._execute_update(query, {
                'page_id': page_id,
                'increment': increment
            })
            
            if rowcount > 0:
                logger.debug(f"incremented views for page {page_id}")
                return True
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to update views for page {page_id}: {e}")
            return False
    
    def update_text(self, page_id: int, new_text: str) -> bool:
        """update page text content"""
        try:
            query = f"""
                UPDATE {self.table_name} 
                SET text = :new_text
                WHERE id = :page_id
            """
            
            rowcount = self._execute_update(query, {
                'page_id': page_id,
                'new_text': new_text
            })
            
            if rowcount > 0:
                logger.info(f"updated text for page {page_id}")
                return True
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to update text for page {page_id}: {e}")
            return False
    
    # DELETE Operations
    
    def delete(self, page_id: int) -> bool:
        """delete a page by ID"""
        try:
            query = f"DELETE FROM {self.table_name} WHERE id = :page_id"
            rowcount = self._execute_update(query, {'page_id': page_id})
            
            if rowcount > 0:
                logger.info(f"deleted page with ID: {page_id}")
                return True
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to delete page {page_id}: {e}")
            return False
    
    def delete_by_title(self, title: str) -> bool:
        """delete a page by title"""
        try:
            page = self.get_by_title(title)
            if page and page.id:
                return self.delete(page.id)
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to delete page with title '{title}': {e}")
            return False
    
    def delete_by_project(self, project_id: int) -> int:
        """delete all pages in a project, returns count deleted"""
        try:
            query = f"DELETE FROM {self.table_name} WHERE project_id = :project_id"
            rowcount = self._execute_update(query, {'project_id': project_id})
            
            logger.info(f"deleted {rowcount} pages from project {project_id}")
            return rowcount
            
        except SQLAlchemyError as e:
            logger.error(f"failed to delete pages for project {project_id}: {e}")
            return 0
    
    # Statistics and Analytics
    
    def count(self) -> int:
        """count total pages"""
        try:
            query = f"SELECT COUNT(*) as count FROM {self.table_name}"
            result = self._execute_query(query)
            return result[0]['count'] if result else 0
            
        except SQLAlchemyError as e:
            logger.error(f"failed to count pages: {e}")
            return 0
    
    def get_statistics(self) -> Dict[str, Any]:
        """get page statistics"""
        try:
            query = """
                SELECT 
                    COUNT(*) as total_pages,
                    SUM(views) as total_views,
                    AVG(views) as avg_views,
                    MAX(views) as max_views,
                    MIN(views) as min_views,
                    COUNT(DISTINCT project_id) as projects_count,
                    COUNT(DISTINCT namespace_id) as namespaces_count
                FROM page
            """
            stats = {}
            result = self._execute_query(query)
            for key, value in result[0].items():
                if isinstance(value, Decimal):
                    stats[key] = float(value)
                else:
                    stats[key] = value
            return stats
        
            
        except SQLAlchemyError as e:
            logger.error(f"failed to get statistics: {e}")
            return {}