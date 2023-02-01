from datetime import datetime


class Location:
    lat: float
    lng: float
    at: datetime


class Vehicle:
    id: str
    location: Location


class OperatingPeriod:
    id: str
    start: datetime
    finish: datetime


class Event:
    event: str
    on: str
    at: datetime
    organization_id: str
    data: object
