from pydantic import BaseModel
from typing import Optional

class BigCommerceProduct(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    sku: Optional[str] = None
    price: Optional[float] = None
    availability: Optional[str] = None

class BigCommerceOrder(BaseModel):
    id: Optional[int] = None
    status_id: Optional[int] = None
    total_inc_tax: Optional[float] = None
    total_ex_tax: Optional[float] = None # Added for consistency
    subtotal_ex_tax: Optional[float] = None # Added for consistency
    customer_id: Optional[int] = None
    date_created: Optional[str] = None
    staff_notes: Optional[str] = None