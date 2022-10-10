import datetime as dt

import datazimmer as dz


class Agency(dz.AbstractEntity):
    agency_id = dz.Index & str
    agency_name = str
    agency_url = str
    agency_timezone = str
    agency_lang = str
    agency_phone = str
    agency_fare_url = str


class Routes(dz.AbstractEntity):
    agency_id = dz.Index & Agency
    route_id = dz.Index & str
    route_short_name = str
    route_long_name = dz.Nullable(str)
    route_type = int


class Shapes(dz.AbstractEntity):
    shape_id = dz.Index & str
    shape_pt_sequence = dz.Index & str
    shape_pt_lat = float
    shape_pt_lon = float
    shape_dist_traveled = float


class Trips(dz.AbstractEntity):
    route_id = dz.Index & Routes
    trip_id = dz.Index & str
    service_id = int
    direction_id = int
    shape_id = Shapes
    wheelchair_accessible = dz.Nullable(float)


class FeedInfo(dz.AbstractEntity):
    feed_publisher_name = dz.Index & str
    feed_publisher_url = str
    feed_lang = str
    feed_start_date = dt.datetime
    feed_end_date = dt.datetime
    feed_version = str


class CalendarDates(dz.AbstractEntity):
    service_id = dz.Index & int
    date = dt.datetime
    exception_type = int


class Stops(dz.AbstractEntity):
    stop_id = dz.Index & str
    stop_name = str
    stop_lat = float
    stop_lon = float
    location_type = dz.Nullable(float)
    parent_station = dz.Nullable(float)


class StopTimes(dz.AbstractEntity):
    trip_id = dz.Index & Trips
    stop_id = dz.Index & Stops
    arrival_time = str
    departure_time = str
    stop_sequence = int
    pickup_type = dz.Nullable(float)
    drop_off_type = dz.Nullable(float)
