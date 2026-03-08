from datetime import datetime

def latest_avail_dump_time(
    current_dt: datetime = datetime.now()
) -> datetime:
    """Returns the latest available time
    rib dumps are available"""

    current_hour = current_dt.hour
    # RIPE dumps every 8 hours, routeview every 2
    hours_to_trim = current_hour % 8
    latest_hour = current_hour - hours_to_trim

    return datetime(
        current_dt.year,
        current_dt.month,
        current_dt.day,
        latest_hour,
        0,
        0
    )

def parse_custom_datetime(
    custom_dt_str:str
) -> datetime:
    """Parses custom datetime from string.
    Expects format as "MM/DD/YYYY"
    If time has not occured yet, uses current time"""

    custom_dt = datetime.strptime(custom_dt_str, "%m/%d/%Y")

    if custom_dt > datetime.now():
        custom_dt = datetime.now()

    return latest_avail_dump_time(
        custom_dt
    )
