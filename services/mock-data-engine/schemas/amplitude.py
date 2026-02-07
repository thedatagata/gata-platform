from pydantic import BaseModel
from typing import Optional

class AmplitudeEvent(BaseModel):
    event_id: Optional[str] = None
    event_type: Optional[str] = None
    user_id: Optional[str] = None
    event_time: Optional[str] = None
    device_type: Optional[str] = None
    country: Optional[str] = None

class AmplitudeUser(BaseModel):
    user_id: Optional[str] = None
    device_type: Optional[str] = None
    country: Optional[str] = None