import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
from KRA_repos.aggregate import app 

client = TestClient(app)

# mock external services
SERVICE_URLS = {
    "category": "http://localhost:8008/categories",
    "page": "http://localhost:8000/pages"
}

# test root endpoint
def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}

# test endpoints with mocked external services
@patch('aggregate.requests.get')
def test_get_category_by_id_success(mock_get):
    # Setup mock response
    mock_response = Mock()
    mock_response.json.return_value = {"id": 1, "name": "Test Category"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    # Make request
    response = client.get("/categories/1")
    
    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Test Category"}

@patch('aggregate.requests.get')
def test_get_category_by_id_not_found(mock_get):
    """test that 404 from external service returns 404 from service"""
    from requests.exceptions import HTTPError
    from unittest.mock import Mock
    
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.json.return_value = {"detail": "Category not found"}
    
    http_error = HTTPError("404 Not Found")
    http_error.response = mock_response
    mock_response.raise_for_status.side_effect = http_error
    
    mock_get.return_value = mock_response
    
    response = client.get("/categories/999")
    
    assert response.status_code == 404

@patch('aggregate.requests.get')
def test_get_page_by_id_success(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {"id": 1, "title": "Test Page", "views": 100}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    response = client.get("/pages/1")
    assert response.status_code == 200
    assert response.json()["title"] == "Test Page"

@patch('aggregate.requests.get')
def test_get_page_stats_success(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {"total_pages": 10, "total_views": 1500}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    response = client.get("/pages/stats")
    assert response.status_code == 200
    assert "total_pages" in response.json()

@patch('aggregate.requests.put')
def test_update_views_success(mock_put):
    mock_response = Mock()
    mock_response.json.return_value = {"id": 1, "views": 101}
    mock_response.raise_for_status.return_value = None
    mock_put.return_value = mock_response
    
    response = client.get("/pages/update_views/1")
    assert response.status_code == 200
    assert response.json()["views"] == 101

@patch('aggregate.requests.delete')
def test_delete_page_success(mock_delete):
    mock_response = Mock()
    mock_response.json.return_value = {"message": "Page deleted successfully"}
    mock_response.raise_for_status.return_value = None
    mock_delete.return_value = mock_response
    
    response = client.get("/pages/delete/1")
    assert response.status_code == 200
    assert "deleted" in response.json()["message"]

# test for network errors
@patch('aggregate.requests.get')
def test_service_unavailable(mock_get):
    from requests.exceptions import RequestException
    mock_get.side_effect = RequestException("Service unavailable")
    
    response = client.get("/categories/1")
    assert response.status_code == 500