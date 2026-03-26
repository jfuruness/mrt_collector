import argparse
from datetime import datetime

OLDEST = datetime(2002, 12, 26, 0, 0, 0)

def get_latest_dl_time() -> datetime:
    """Returns the closest available time
    rib dumps are available to current_dt,
    which defaults to the current time
    """

    current_dt = datetime.now()
    current_hour = current_dt.hour
    # RIPE dumps every 8 hours, routeview every 2
    hours_to_trim = current_hour % 8
    latest_hour = current_hour - hours_to_trim

    latest = datetime(
        current_dt.year,
        current_dt.month,
        current_dt.day,
        latest_hour,
        0,
        0
    )
    print(f"Latest available RIB dump is from {latest}")
    return latest

def handle_datetime(
    custom_dt_str:str
) -> datetime:
    """Parses custom datetime from string.
    Expects format as "MM/DD/YYYY/HH"
    If time has not occured yet, uses current time
    """

    try:
        given_dt = datetime.strptime(custom_dt_str, "%m/%d/%Y/%H")
    except ValueError:
        raise argparse.ArgumentTypeError( # noqa
            f"Invalid datetime: '{custom_dt_str}'. Expected format: mm/dd/yyyy/hh"
        )

    # RIPE dumps every 8 hours, routeview every 2
    if given_dt.hour % 8 != 0:
        raise ValueError("RIPE dumps every 8 hours, routeview every 2; given hours must be 00, 08, or 16") # noqa
    elif given_dt > datetime.now():
        raise ValueError("Given time has yet to occur")
    elif given_dt < OLDEST:
        raise ValueError(f"Given time predates RIB dumps, oldest available is {OLDEST}.")

    return given_dt

