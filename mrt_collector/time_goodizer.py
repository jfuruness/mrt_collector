from datetime import datetime

OLDEST = datetime(2001, 10, 27, 0, 0, 0)

def get_dl_time(
    current_dt: datetime = datetime.now()
) -> datetime:
    """Returns the closest available time
    rib dumps are available to current_dt,
    which defaults to the current time"""

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
    Expects format as "MM/DD/YYYY/HH"
    If time has not occured yet, uses current time"""
    
    try:
        custom_dt = datetime.strptime(custom_dt_str, "%m/%d/%Y/%H")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime: '{custom_dt_str}'. Expected format: mm/dd/yyyy/hh"
        )

    if custom_dt > datetime.now():
        custom_dt = datetime.now()
        print(f"Submitted time is in the future, using latest available, {custom_dt}.")
    elif custom_dt < OLDEST:
        custom_dt = OLDEST
        print(f"Submitted time predates RIB dumps, using oldest available, {custom_dt}.")

    return get_dl_time(
        custom_dt
    )
