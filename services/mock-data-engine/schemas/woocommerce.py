from pydantic import BaseModel
from typing import Optional, List, Any
from datetime import datetime

class WooCommerceProduct(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    slug: Optional[str] = None
    type: Optional[str] = "simple"
    status: Optional[str] = "publish"
    price: Optional[float] = None
    regular_price: Optional[float] = None
    sku: Optional[str] = None
    stock_quantity: Optional[int] = None
    categories: Optional[str] = None
    created_date_gmt: Optional[datetime] = None

class WooCommerceOrder(BaseModel):
    id: Optional[int] = None
    number: Optional[str] = None
    status: Optional[str] = None
    currency: Optional[str] = "USD"
    total: Optional[float] = None
    subtotal: Optional[float] = None
    total_tax: Optional[float] = 0.0
    payment_method: Optional[str] = "stripe"
    payment_method_title: Optional[str] = "Credit Card (Stripe)"
    date_created_gmt: Optional[datetime] = None
    date_modified_gmt: Optional[datetime] = None
    billing_email: Optional[str] = None
    billing_first_name: Optional[str] = None
    billing_last_name: Optional[str] = None
    customer_id: Optional[int] = None
    line_items: Optional[List[dict]] = None
    meta_data: Optional[List[dict]] = None