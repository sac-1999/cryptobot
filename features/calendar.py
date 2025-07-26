from datetime import datetime, timedelta
from typing import Union, List

DateType = Union[str, datetime, datetime.date]

def to_date(date_input):
    if isinstance(date_input, str):
        return datetime.strptime(date_input, "%Y-%m-%d").date()
    elif isinstance(date_input, datetime):
        return date_input.date()
    elif isinstance(date_input, datetime.date):
        return date_input
    else:
        raise ValueError("Unsupported date format. Use 'YYYY-MM-DD', datetime, or date object.")

def get_last_training_date(target_date, frequency = 'daily'):
    """
    Returns the most recent model training date on or before the target date.
    """
    target_date = to_date(target_date)

    if frequency == 'daily':
        return target_date

    elif frequency == 'weekly':
        # Return most recent Monday
        return target_date - timedelta(days=target_date.weekday())

    elif frequency == 'monthly':
        # Return 1st of the current month
        return target_date.replace(day=1)

    else:
        raise ValueError("Invalid frequency. Use 'daily', 'weekly', or 'monthly'.")

def get_training_dates(target_date, lookback_days, lag = 2, frequency= 'daily'):
    """
    Returns a list of training dates within the lookback window.
    """
    target_date = to_date(target_date)
    start_date = target_date - timedelta(days=lookback_days)
    training_dates = []
    date = target_date -timedelta(days = lag)
    current = start_date
    while current <= date:
        if frequency == 'daily':
            training_dates.append(current)
            current += timedelta(days=1)

        elif frequency == 'weekly':
            if current.weekday() == 0:  # Monday
                training_dates.append(current)
            current += timedelta(days=1)

        elif frequency == 'monthly':
            if current.day == 1:
                training_dates.append(current)
            current += timedelta(days=1)

        else:
            raise ValueError("Invalid frequency. Use 'daily', 'weekly', or 'monthly'.")

    return training_dates
