from datetime import datetime, timedelta


def delay_delete_processing(**kwargs):
    """
    Checks to see if the execution start time + delay has passed.
    If so, return true to continue processing opt out / delete.
    If not, check back later.
    """

    start_time_str = kwargs['start_date']
    print(start_time_str)
    start_time = (datetime.fromisoformat(start_time_str))
    # Ensure current_time is timezone-aware
    current_time = datetime.now(tz=start_time.tzinfo)
    elapsed_time = current_time - start_time
    if elapsed_time >= timedelta(hours=24):
        return True
    return False
