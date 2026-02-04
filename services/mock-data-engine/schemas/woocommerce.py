from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class WooCommerceProduct(BaseModel):
    id: int
    name: str
    slug: str
    type: str = "simple"
    status: str = "publish"
    price: str
    regular_price: str
    sku: str
    stock_quantity: Optional[int] = None
    categories: str  # JSON string of category names
    created_date_gmt: datetime

class WooCommerceOrder(BaseModel):
    id: int
    number: str
    status: str  # processing, completed, refunded
    currency: str = "USD"
    total: str  # string like Shopify
    subtotal: str
    total_tax: str = "0.00"
    payment_method: str = "stripe"
    payment_method_title: str = "Credit Card (Stripe)"
    date_created_gmt: datetime
    date_modified_gmt: datetime
    billing_email: str
    billing_first_name: str
    billing_last_name: str
    customer_id: int
    line_items: str  # JSON string of line items
    meta_data: str  # JSON string — include stripe_charge_id here
