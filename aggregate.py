from fastapi import FastAPI, HTTPException
import requests
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CATEGORY_SERVICE_URL = 'http://localhost:8008/categories'
PAGE_SERVICE_URL = 'http://localhost:8000/pages'

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/categories/{id}")
async def get_category_by_id(id):
    try:
        response = requests.get(CATEGORY_SERVICE_URL + f"/{id}")
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error occurred: {err}")
        raise HTTPException(status_code=err.response.status_code, detail=str(err))
    
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        raise HTTPException(status_code=500, detail=str(err))
    
@app.get("/pages/{id}")
async def get_page_by_id(id):
    try:
        response = requests.get(PAGE_SERVICE_URL + f"/{id}")
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error occurred: {err}")
        raise HTTPException(status_code=err.response.status_code, detail=str(err))
    
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        raise HTTPException(status_code=500, detail=str(err))
    
@app.get("/pages/stats")
async def get_page_stats():
    try:
        response = requests.get(PAGE_SERVICE_URL + f"/stats")
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error occurred: {err}")
        raise HTTPException(status_code=err.response.status_code, detail=str(err))
    
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        raise HTTPException(status_code=500, detail=str(err))
    
@app.get("/pages/update_views/{id}")
async def update_views(id):
    try:
        response = requests.put(PAGE_SERVICE_URL + f"/{id}")
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error occurred: {err}")
        raise HTTPException(status_code=err.response.status_code, detail=str(err))
    
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        raise HTTPException(status_code=500, detail=str(err))

@app.get("/pages/delete/{id}")
async def delete_page(id):
    try:
        response = requests.delete(PAGE_SERVICE_URL + f"/delete/{id}")
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error occurred: {err}")
        raise HTTPException(status_code=err.response.status_code, detail=str(err))
    
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        raise HTTPException(status_code=500, detail=str(err))