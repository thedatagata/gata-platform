from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class BigCommerceProduct(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    sku: Optional[str] = None
    price: Optional[float] = None
    availability: Optional[str] = "available"

class BigCommerceOrder(BaseModel):
    id: Optional[int] = None
    status: Optional[str] = None 
    status_id: Optional[int] = None 
    total_price: Optional[float] = None 
    currency: Optional[str] = "USD"
    customer_id: Optional[int] = None
    staff_notes: Optional[str] = None
    created_at: Optional[datetime] = None 