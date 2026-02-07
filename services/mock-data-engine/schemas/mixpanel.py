from pydantic import BaseModel, Field
from typing import Optional

class MixpanelEvent(BaseModel):
    event: Optional[str] = None
    properties: Optional[dict] = None

class MixpanelPerson(BaseModel):
    distinct_id: Optional[str] = Field(None, alias="$distinct_id")
    city: Optional[str] = Field(None, alias="$city")
    email: Optional[str] = Field(None, alias="$email")