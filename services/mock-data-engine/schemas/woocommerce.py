from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class WooCommerceProduct(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    sku: Optional[str] = None
    price: Optional[float] = None
    status: Optional[str] = "publish"
    created_at: Optional[datetime] = None

class WooCommerceOrder(BaseModel):
    id: Optional[int] = None
    number: Optional[str] = None
    status: Optional[str] = None # Standardized
    currency: Optional[str] = "USD"
    total_price: Optional[float] = None # Standardized from total
    customer_id: Optional[int] = None
    line_items: Optional[List[dict]] = None
    created_at: Optional[datetime] = None # Standardized from date_created_gmt