from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional, Any

class ShopifyProduct(BaseModel):
    id: int
    title: str
    product_type: str
    status: str
    variants: List[dict]
    created_at: datetime

class ShopifyOrder(BaseModel):
    id: int
    name: str
    email: str
    created_at: datetime
    processed_at: datetime
    updated_at: datetime
    total_price: str
    subtotal_price: str
    currency: str
    financial_status: str
    customer: dict
    line_items: List[dict]
    note_attributes: List[dict] # Critical for Stripe Link
    payment_gateway_names: List[str]