from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List

class StripeCharge(BaseModel):
    id: str
    amount: int
    amount_captured: int
    amount_refunded: int
    currency: str
    created: datetime
    status: str
    paid: bool
    payment_method_details: dict
    refunded: bool