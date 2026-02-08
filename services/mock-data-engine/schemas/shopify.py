from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class ShopifyProduct(BaseModel):
    id: Optional[int] = None
    title: Optional[str] = None
    product_type: Optional[str] = "Mock Product"
    status: Optional[str] = "active"
    price: Optional[float] = None
    created_at: Optional[datetime] = None

class ShopifyOrder(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    email: Optional[str] = None
    total_price: Optional[float] = None # Standardized
    currency: Optional[str] = "USD"
    financial_status: Optional[str] = "paid"
    status: Optional[str] = "fulfilled" # Standardized
    customer: Optional[dict] = None
    line_items: Optional[List[dict]] = None
    created_at: Optional[datetime] = None # Standardized