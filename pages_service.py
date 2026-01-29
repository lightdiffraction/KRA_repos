from fastapi import FastAPI
import dataclasses
import json
# импорт кlассов репозитория
from repositories.page_repository import PageRepository, Page
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
pageRepository = PageRepository(DB_URL)

@app.get("/")
async def root():
    return {"message": "Hello World"}

#read
@app.get("/pages/stats")
async def get_page_stats():
    result = pageRepository.get_statistics()
    if result is not None:
        return json.dumps(result)
    else:
        return None
    
#read
@app.get("/pages/{id}")
async def get_page_by_id(id):
    result = pageRepository.get_by_id(id)
    if result is not None:
        return json.dumps(dataclasses.asdict(result, dict_factory=dict))
    else:
        return None
    

    
#update
@app.put("/pages/update_views/{id}")
async def update_views(id):
    result = pageRepository.update_views(id)
    if result is not None:
        return json.dumps(result)
    else:
        return None

#delete
@app.delete("/pages/delete/{id}")
async def delete_page(id):
    result = pageRepository.delete(id)
    if result is not None:
        return json.dumps(result)
    else:
        return None