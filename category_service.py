from fastapi import FastAPI
import dataclasses
import json
# импорт кlассов репозитория
from repositories.category_repository import CategoryRepository, Category
from repositories.base_repository import DatabaseConnection

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
DB_URL = "postgresql://postgres:postgres@localhost:5432/postgres"
categoryRepository = CategoryRepository(DB_URL)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/categories/{id}")
async def get_category_by_id(id):
    result = categoryRepository.get_by_id(id)
    if result is not None:
        return json.dumps(dataclasses.asdict(result, dict_factory=dict))
    else:
        return None
    
@app.get("/categories/search/{keyword}")
async def get_category_by_keyword(keyword):
    result = categoryRepository.search(keyword)
    if result is not None:
        return json.dumps(result)
    else:
        return None