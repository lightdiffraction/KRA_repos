from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import json
from .base_repository import BaseRepository, Category, Page_Category, DatabaseConnection
from sqlalchemy.exc import SQLAlchemyError
import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CategoryRepository(BaseRepository[Category]):
    """repository for category table operations"""
    
    def __init__(self, db_url: str):
        super().__init__(db_url)
        self.table_name = "category"
    
    def _to_entity(self, row: Dict) -> Category:
        """convert database row to Category entity"""
        return Category(
            id=row.get('id'),
            name=row.get('name', ''),
            status=row.get('status', 'stub'),
            text_content=row.get('text_content', '')
        )
    
    def _from_entity(self, category: Category) -> Dict:
        """convert Category entity to database row"""
        data = dict[str, Any]
        data = {
            'name': category.name,
            'status': category.status,
            'text_content': category.text_content,
            'id': None
        }
        
        if category.id:
            data['id'] = category.id
            
        return {k: v for k, v in data.items() if v is not None}
    
    # CREATE Operations
    
    def create(self, category: Category) -> Optional[Category]:
        """create a new category"""
        try:
            # Check if category with same name exists
            if self.get_by_name(category.name):
                logger.warning(f"category with name '{category.name}' already exists")
                return None
            
            data = self._from_entity(category)
            
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
                        category.id = row.id
                        logger.info(f"created category with ID: {category.id}")
                        return category
            
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"failed to create category: {e}")
            return None
    
    def create_batch(self, categories: List[Category]) -> List[Category]:
        """create multiple pages at once"""
        created_categories = []
        for category in categories:
            created = self.create(category)
            if created:
                created_categories.append(created)
        return created_categories
    
    def link_page_to_category(self, page_id, category_id) -> Optional[Page_Category]:
        "adds a row to page_category linking a page to a category"
        data = {
            'page_id': page_id,
            'category_id': category_id
        }
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join([f':{key}' for key in data.keys()])
            
            query = f"""
                INSERT INTO page_category ({columns})
                VALUES ({placeholders})
            """
            
            with self.db.get_session() as session:
                result = session.execute(text(query), data)
                session.commit()
                    
                if result:
                    
                    return Page_Category(page_id=page_id, category_id=category_id)
        except SQLAlchemyError as e:
            logger.error(f"failed to link page to category: {e}")
            return None
    
    # READ Operations
    
    def get_by_id(self, category_id: int) -> Optional[Category]:
        """get category by ID"""
        try:
            query = f"""
                SELECT c.*, 
                       name,
                       text_content
                FROM {self.table_name} c
                WHERE c.id = :category_id
            """
            
            result = self._execute_query(query, {'category_id': category_id})
            if result:
                return self._to_entity(result[0])
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get category by ID {category_id}: {e}")
            return None
    
    def get_by_name(self, name: str) -> Optional[Category]:
        """get category by name (case-insensitive)"""
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                WHERE LOWER(name) = LOWER(:name)
                LIMIT 1
            """
            
            result = self._execute_query(query, {'name': name})
            if result:
                return self._to_entity(result[0])
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get category by name '{name}': {e}")
            return None
    
    def get_all(self, limit: int = 100, offset: int = 0) -> List[Category]:
        """get all categories with pagination"""
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                ORDER BY id 
                LIMIT :limit OFFSET :offset
            """
            
            result = self._execute_query(query, {'limit': limit, 'offset': offset})
            return [self._to_entity(row) for row in result]
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get all categories: {e}")
            return []
    
    def search(self, keyword: str, limit: int = 50) -> List[Category]:
        """search categories by keyword in title or text"""
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                WHERE name ILIKE :keyword 
                   OR text_content ILIKE :keyword
                ORDER BY 
                    CASE 
                        WHEN name ILIKE :keyword_exact THEN 1
                        WHEN text_content ILIKE :keyword_exact THEN 2
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
            logger.error(f"failed to search categories with keyword '{keyword}': {e}")
            return []
    
    def update(self, category: Category) -> bool:
        """update an existing category"""
        try:
            if not category.id:
                logger.error("cannot update category without ID")
                return False
            
            data = self._from_entity(category)
            
            set_clause = ', '.join([f"{key} = :{key}" for key in data.keys()])
            
            query = f"""
                UPDATE {self.table_name} 
                SET {set_clause}
                WHERE id = :id
            """
            
            data['id'] = 'DEFAULT'
            rowcount = self._execute_update(query, data)
            
            if rowcount > 0:
                logger.info(f"updated category with ID: {category.id}")
                return True
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to update category {category.id}: {e}")
            return False
    
    def update_text_content(self, category_id: int, new_text: str) -> bool:
        """update category text content"""
        try:
            query = f"""
                UPDATE {self.table_name} 
                SET text_content = :new_text
                WHERE id = :category_id
            """
            
            rowcount = self._execute_update(query, {
                'category_id': category_id,
                'new_text': new_text
            })
            
            if rowcount > 0:
                logger.info(f"updated text for category {category_id}")
                return True
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to update text for category {category_id}: {e}")
            return False
    
    # DELETE Operations
    
    def delete(self, category_id: int) -> bool:
        """delete a category by ID"""
        try:
            query = f"DELETE FROM {self.table_name} WHERE id = :category_id"
            rowcount = self._execute_update(query, {'category_id': category_id})
            
            if rowcount > 0:
                logger.info(f"deleted category with ID: {category_id}")
                return True
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to delete category {category_id}: {e}")
            return False
    
    def delete_by_title(self, name: str) -> bool:
        """delete a category by name"""
        try:
            category = self.get_by_name(name)
            if category and category.id:
                return self.delete(category.id)
            return False
            
        except SQLAlchemyError as e:
            logger.error(f"failed to delete category with name '{name}': {e}")
            return False
    
    # statistics and analytics
    
    def count(self) -> int:
        """count total categories"""
        try:
            query = f"SELECT COUNT(*) as count FROM {self.table_name}"
            result = self._execute_query(query)
            return result[0]['count'] if result else 0
            
        except SQLAlchemyError as e:
            logger.error(f"failed to count categories: {e}")
            return 0
    
    def get_or_create_by_name(self, name: str) -> Optional[Category]:
        '''gets category by name or creates if it doesn't exist'''
        category = self.get_by_name(name)
        if category == None:
            logger.warning(f"creating empty category with the name {name}")
            self.create(Category(name=name))
            return self.get_by_name(name)
        else:
            return category
        
    
    