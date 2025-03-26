# NOTE: this is part of this repo but it could come from a package to allow for
# more easy schema updates.

from datetime import datetime
from typing import List, Literal
from pydantic import BaseModel, Field


class MediaEvent(BaseModel):
    # Event core fields
    event_id: str = Field(
        ...,
        description="Unique identifier for the event",
    )
    event_type: Literal["play", "pause", "stop"] = Field(
        ...,
        description="Type of media event that occurred",
    )
    timestamp: str = Field(
        ...,
        description="Timestamp of when the event occurred",
    )

    # User fields
    user_id: str = Field(
        ..., description="The user ID as identified by a authentication provider"
    )
    device_id: str = Field(..., description="ID to identify the users device")
    ip_address: str = Field(..., description="Users IP")

    # Content fields
    content_id: str = Field(
        ...,
        description="An unique ID to identify this content e.g UUID",
    )
    content_type: Literal["movie", "episode", "ad"] = Field(
        ...,
        description="The content types which are available",
    )
    title: str = Field(
        ...,
        description="The title of the content",
    )
    genre: List[str] = Field(
        ...,
        description="List of genres associated with the content",
    )
    duration_seconds: int = Field(
        ...,
        description="Duration of the content in seconds",
    )
    language: str = Field(
        ...,
        description="Language code of the content",
    )

    # Event data fields
    current_timestamp: float = Field(..., description="Playback progress in seconds")

    # Metadata field
    schema_version: str = Field(
        default="1.0", description="Schema version for the event"
    )
    event_source: str = Field(
        ..., description="Source of the event (e.g., web_app, mobile_app)"
    )

    # Allow for additional fields
    model_config = {
        "extra": "allow",
    }
