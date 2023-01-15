
import calendar
from datetime import datetime


def month_to_number(month_name, locale='es'):
    """Return the number of the month for the given month name.

    Args:
        month_name (str): The name of the month.

    Returns:
        Int: The number of the month.

    Raises:
        ValueError: If the month name is not valid.

    """
    if locale == 'es':
        months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']
    elif locale == 'en':
        months = [month.lower() for month in calendar.month_name if month]
    else:
        raise ValueError("Locale is not valid")
    if month_name.lower() not in months:
        raise ValueError("Month name is not valid")
    return months.index(month_name.lower()) + 1


def month_name(month_number):
    """Return the name of the month for the given month number.

    Args:
        month_number (int): The number of the month.

    Returns:
        str: The name of the month.

    Raises:
        ValueError: If the month number is not between 1 and 12.

    """
    if month_number < 1 or month_number > 12:
        raise ValueError("Month number must be between 1 and 12")
    return {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December"
    }[month_number]

def datetime_from_str(date_string):
    """
    Return a datetime object from a string.
    """
    return datetime.strptime(date_string,'%d-%b-%y')