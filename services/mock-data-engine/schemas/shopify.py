from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional, Any

class ShopifyProduct(BaseModel):
    id: Optional[int] = None
    title: Optional[str] = None
    product_type: Optional[str] = None
    status: Optional[str] = None
    variants: Optional[List[dict]] = None
    created_at: Optional[datetime] = None

class ShopifyOrder(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    email: Optional[str] = None
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    total_price: Optional[float] = None
    subtotal_price: Optional[float] = None
    currency: Optional[str] = None
    financial_status: Optional[str] = None
    customer: Optional[dict] = None
    line_items: Optional[List[dict]] = None
    note_attributes: Optional[List[dict]] = None
    payment_gateway_names: Optional[List[str]] = None