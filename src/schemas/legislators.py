from pydantic import BaseModel, Field


class LegislatorSchema(BaseModel):
    bioguideId: str = Field(min_length=3)
    name: str | None = None
    partyName: str | None = None
    state: str = Field(min_length=1)
