
import json
import asyncio
from typing import Optional, List, Any, Dict, AsyncGenerator

try:
    import httpx
except ImportError:
    raise ImportError("The 'httpx' library is required. Please install it with: pip install httpx")

class PrkDbClient:
    def __init__(self, host: str = "http://127.0.0.1:8080"):
        self.host = host.rstrip('/')
        self.client = httpx.AsyncClient()
    
    async def close(self):
        await self.client.aclose()
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def list(self, collection: str, limit: int = 100, offset: int = 0, filter: Optional[str] = None, sort: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"limit": limit, "offset": offset}
        if filter:
            params["filter"] = filter
        if sort:
            params["sort"] = sort
            
        response = await self.client.get(f"{self.host}/collections/{collection}/data", params=params)
        
        if response.status_code == 200:
            data = response.json()
            # Response is wrapped in {"success": true, "data": ...}
            result = data.get("data", {})
            # For data endpoints, the actual list is wrapped in the result
            if isinstance(result, dict) and "data" in result and isinstance(result["data"], list):
                return result["data"]
            return result if isinstance(result, list) else []
        else:
            raise Exception(f"Failed to list collection: {response.status_code}")

    async def put(self, collection: str, data: Dict[str, Any]) -> None:
        """Insert or update a record in the collection"""
        response = await self.client.put(
            f"{self.host}/collections/{collection}/data",
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code not in (200, 201):
            raise Exception(f"Failed to put record: {response.status_code}")

    async def delete(self, collection: str, id: str) -> None:
        """Delete a record from the collection"""
        response = await self.client.delete(f"{self.host}/collections/{collection}/data/{id}")
        if response.status_code != 200:
            raise Exception(f"Failed to delete record: {response.status_code}")

    async def replay_collection(self, collection: str, handler):
        """
        Replay all events/items in a collection and apply them to a stateful handler.
        Uses streaming to avoid loading all data into memory.
        
        Handler must implement:
          - init_state(self) -> state
          - handle(self, state, event) -> void (modifies state in place)
        """
        limit = 100
        offset = 0
        state = handler.init_state()
        
        while True:
            items = await self.list(collection, limit=limit, offset=offset)
            if not items:
                break
            
            for item in items:
                 handler.handle(state, item)
            
            if len(items) < limit:
                break
                
            offset += len(items)
            
        return state
        
    async def stream(self, collection: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream all records from a collection using an async generator"""
        limit = 100
        offset = 0
        
        while True:
            items = await self.list(collection, limit=limit, offset=offset)
            if not items:
                break
                
            for item in items:
                yield item
                
            if len(items) < limit:
                break
                
            offset += len(items)

class QueryBuilder:
    def __init__(self, model_cls, collection_name: str):
        self.model_cls = model_cls
        self.collection_name = collection_name
        self.filters = []
        self.sort_field = None
        self._limit = 100
        self._offset = 0

    def filter(self, field, op, value):
        self.filters.append(f"{field}{op}{value}")
        return self
        
    def sort(self, field, desc=False):
        suffix = ":desc" if desc else ":asc"
        self.sort_field = f"{field}{suffix}"
        return self
        
    def limit(self, limit):
        self._limit = limit
        return self
        
    def offset(self, offset):
        self._offset = offset
        return self

    async def execute(self, client: PrkDbClient) -> List[Any]:
        items = await client.list(
            self.collection_name, 
            limit=self._limit, 
            offset=self._offset, 
            filter=",".join(self.filters) if self.filters else None,
            sort=self.sort_field
        )
        # Convert dicts to model objects
        return [self.model_cls.from_dict(item) for item in items]
