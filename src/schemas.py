from typing import List, Optional
from pydantic import BaseModel

class Link(BaseModel):
    id: str
    name: Optional[str] = None
    open: Optional[bool] = None
    links: Optional[List['Link']] = None
    url: Optional[str] = None
    checked: Optional[bool] = None

Link.model_rebuild()

class Bookmark(BaseModel):
    id: str
    name: Optional[str] = None
    open: Optional[bool] = None
    links: Optional[List[Link]] = None
    checked: Optional[bool] = None

class Record(BaseModel):
    id: str
    user_id: str
    bookmarks: Optional[List[Bookmark]] = None
    created_at: str

class Payload(BaseModel):
    type: str
    table: str
    record: Record
    schema: str
    old_record: Optional[Record] = None