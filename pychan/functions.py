import re

from datetime import (
    date,
    datetime,
    timedelta
)
from dateutil import parser
from dateutil.relativedelta import relativedelta

from pychan.types import (
    String,
    Boolean,
    Timestamp,
    Int,
    Date,
    Map,
    Unit
)

_TIMESTAMP_PART: Map[String, String] = {
    "year": "years",
    "month": "months",
    "day": "days",
    "week": "weeks",
    "hour": "hours",
    "minute": "minutes",
    "second": "seconds",
    "microsecond": "microseconds"
}

_TIMESTAMP_TRUNC = {
    "year": lambda x: x.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
    "month": lambda x: x.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
    "day": lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0),
    "week": lambda x: (x - timedelta(days=x.weekday())).replace(hour=0, minute=0, second=0, microsecond=0),
    "hour": lambda x: x.replace(minute=0, second=0, microsecond=0),
    "minute": lambda x: x.replace(second=0, microsecond=0),
    "second": lambda x: x.replace(microsecond=0)
}

_DATE_TRUNC = {
    "year": lambda x: x.replace(month=1, day=1),
    "month": lambda x: x.replace(day=1)
}

class PartError(Exception):
    def __init__(self, message: String):
        super().__init__(message)

class ParserError(Exception):
    def __init__(self, message: String):
        super().__init__(message)

def _assertString(
    string: Timestamp,
    funcName: String
) -> Unit:
    if not isinstance(string, str):
        raise TypeError(f"{__name__}.{funcName}()")

def _assertTimestamp(
    timestampExpr: Timestamp,
    funcName: String
) -> Unit:
    if not isinstance(timestampExpr, datetime):
        raise TypeError(f"{__name__}.{funcName}()")

def _assertDate(
    dateExpr: Date,
    funcName: String
) -> Unit:
    if (
        not isinstance(dateExpr, date)
        or isinstance(dateExpr, datetime)
    ):
        raise TypeError(f"{__name__}.{funcName}()")

def _shiftTime(
    expr: Date | Timestamp,
    number,
    part,
    funcName: String,
    add: Boolean = True
) -> Date | Timestamp:
    parts: String = _TIMESTAMP_PART.get(part)
    if not parts:
        raise PartError(f"{__name__}.{funcName}()")
    
    delta = relativedelta(**{parts: number})
    
    return expr + delta if add else expr - delta

def _parseTimestamp(
    timeStr: String,
    funcName: String
) -> Timestamp:
    _assertString(timeStr, funcName)
    
    try:
        return parser.parse(timeStr)
    except (ValueError, TypeError):
        raise ParserError(f"{__name__}.{funcName}()")

def getCurrentTimestamp() -> Timestamp:
    """
    >>> currentTimestamp = getCurrentTimestamp()
    >>> currentTimestamp
    datetime.datetime(2025, 7, 30, 9, 57, 12, 912564)
    """
    return datetime.utcnow()

def getCurrentTime() -> String:
    """
    >>> getCurrentTime()
    "06:32:30"
    """
    return timestampFormat(getCurrentTimestamp(), "%H:%M:%S")

def timestampAdd(
    timestampExpr: Timestamp,
    number: Int,
    part: String
) -> Timestamp:
    """
    >>> timestampAdd(currentTimestamp, 1, "day")
    datetime.datetime(2025, 7, 31, 9, 57, 12, 912564)
    """
    _assertTimestamp(timestampExpr, "timestampAdd")
    return _shiftTime(timestampExpr, number, part, "timestampAdd")

def timestampSub(
    timestampExpr: Timestamp,
    number: Int,
    part: String
) -> Timestamp:
    """
    >>> timestampSub(currentTimestamp, 1, "day")
    datetime.datetime(2025, 7, 29, 9, 57, 12, 912564)
    """
    _assertTimestamp(timestampExpr, "timestampSub")
    return _shiftTime(timestampExpr, number, part, "timestampSub", add=False)

def timestampDiff(
    endTimestampExpr: Timestamp,
    startTimestampExpr: Timestamp
) -> Int:
    """
    >>> timestampDiff(currentTimestamp, timestampSub(currentTimestamp, 1, "day"))
    1
    """
    _assertTimestamp(endTimestampExpr, "timestampDiff")
    _assertTimestamp(startTimestampExpr, "timestampDiff")
    
    return (endTimestampExpr - startTimestampExpr).days

def timestampTrunc(
    timestampExpr: Timestamp,
    part: String
) -> Timestamp:
    """
    >>> timestampTrunc(currentTimestamp, "minute")
    datetime.datetime(2025, 7, 30, 9, 57)
    """
    _assertTimestamp(timestampExpr, "timestampTrunc")
    
    func = _TIMESTAMP_TRUNC.get(part)
    if not func:
        raise PartError(f"{__name__}.timestampTrunc()")
    
    return func(timestampExpr)

def timestampFormat(
    timestampExpr: Timestamp,
    format: String
) -> String:
    """
    >>> timestampFormat(currentTimestamp, "%Y-%m-%d %H:%M:%S")
    '2025-02-20 16:09:32'
    >>> timestampFormat(currentTimestamp, "%Y%m%d")
    '20250220'
    """
    _assertTimestamp(timestampExpr, "timestampFormat")
    return timestampExpr.strftime(format)

def toTimestamp(dateOrTimestampStr: Date | String) -> Timestamp:
    """
    >>> toTimestamp("2025-07-30 09:44:36.445610"
    datetime.datetime(2025, 7, 30, 9, 44, 36, 445610)
    >>> toTimestamp(currentDate)
    datetime.datetime(2025, 7, 30, 0, 0)
    """
    if isinstance(dateOrTimestampStr, str):
        return datetime.fromisoformat(dateOrTimestampStr)
    if isinstance(dateOrTimestampStr, date):
        return datetime.combine(dateOrTimestampStr, datetime.min.time())
    raise TypeError(f"{__name__}.toTimestamp()")

def toTimestampId(timestampStr: String) -> String:
    """
    >>> toTimestampId("2025-05-27 13:05:09")
    "20250527130509"
    >>> toTimestampId("2025-02-20 16:09:32.994308")
    "20250220160932"
    """
    _ = _parseTimestamp(timestampStr, "toTimestampId")
    return re.sub(r"\D", "", timestampStr[:19])

def parseTimestamp(timeStr: String) -> Timestamp:
    """
    >>> parseTimestamp("2025-07-30 10:30:42.130807")
    datetime.datetime(2025, 7, 30, 10, 30, 42, 130807)
    
    >>> parseTimestamp("2025-07-30")
    datetime.datetime(2025, 7, 30, 0, 0)
    
    >>> parseTimestamp("20250527130509")
    datetime.datetime(2025, 5, 27, 13, 5, 9)
    
    >>> parseTimestamp("2025-07-30 10:304")
    ParserError: pytyk.functions.parseTimestamp()
    """
    return _parseTimestamp(timeStr, "parseTimestamp")

def getCurrentDate() -> Date:
    """
    >>> getCurrentDate()
    datetime.date(2025, 7, 30)
    """
    return getCurrentTimestamp().date()

def dateAdd(
    dateExpr: Date,
    number: Int,
    part: String
) -> Date:
    """
    >>> dateAdd(currentDate, 1, "day")
    datetime.date(2025, 2, 21)
    """
    _assertDate(dateExpr, "dateAdd")
    return _shiftTime(dateExpr, number, part, "dateAdd")

def dateSub(
    dateExpr: Date,
    number: Int,
    part: String
) -> Date:
    """
    >>> dateSub(current_date, 1, "day")
    datetime.date(2025, 2, 19)
    """
    _assertDate(dateExpr, "dateSub")
    return _shiftTime(dateExpr, number, part, "dateSub", add = False)

def dateDiff(
    endDateExpr: Date,
    startDateExpr: Date
) -> Int:
    """
    >>> dateDiff(currentDate, dateDiff(currentDate, 1, "day"))
    1
    """
    _assertDate(endDateExpr, "dateDiff")
    _assertDate(startDateExpr, "dateDiff")
    
    return (endDateExpr - startDateExpr).days

def dateTrunc(
    dateExpr: Date,
    part: String
) -> Date:
    """
    >>> dateTrunc(current_date, "month")
    datetime.date(2025, 2, 1)
    """
    _assertDate(dateExpr, "dateTrunc")
    
    func = _DATE_TRUNC.get(part)
    if not func:
        raise PartError(f"{__name__}.dateTrunc()")
    
    return func(dateExpr)

def dateFormat(
    dateExpr: Date,
    format: String
) -> String:
    """
    >>> dateFormat(currentDate, "%Y-%m-%d")
    "2025-02-20"
    
    >>> dateFormat(currentDate, "%Y%m%d")
    "20250220"
    """
    _assertDate(dateExpr, "dateFormat")
    return dateExpr.strftime(format)

def toDate(timestampOrDateStr: Timestamp | String) -> Date:
    """
    >>> toDate("2025-02-20 16:09:32.994308")
    datetime.date(2025, 2, 20)
    """
    if isinstance(timestampOrDateStr, str):
        return datetime.fromisoformat(timestampOrDateStr).date()
    if isinstance(timestampOrDateStr, datetime):
        return timestampOrDateStr.date()
    raise TypeError(f"{__name__}.toDate()")

def toDateId(timestampStrOrDateStr: String) -> String:
    """
    >>> toDateId("2025-05-27 13:05:09")
    "20250527"
    
    >>> toDateId("2025-02-20")
    "20250220"
    """
    _ = _parseTimestamp(timestampStrOrDateStr, "toTimestampId")
    return re.sub(r"\D", "", timestampStrOrDateStr[:10])

def parseDate(timeStr: String) -> Date:
    return _parseTimestamp(timeStr, "parseDate").date()

def getYear(timestampOrDate: Timestamp | Date) -> Int:
    """
    >>> getYear(currentDate)
    2025
    """
    return timestampOrDate.year

def getMonth(timestampOrDate: Timestamp | Date) -> Int:
    """
    >>> getMonth(currentDate)
    1
    """
    return timestampOrDate.month

def getMonthName(timestampOrDate: Timestamp | Date) -> String:
    """
    >>> getMonthName(currentDate)
    'January'
    """
    return timestampOrDate.strftime("%B")

def getDay(timestampOrDate: Timestamp | Date) -> Int:
    """
    >>> getDay(currentDate)
    8
    """
    return timestampOrDate.day

def getDayName(timestampOrDate: Timestamp | Date) -> String:
    """
    >>> getDayName(currentDate)
    'Wednesday'
    """
    return timestampOrDate.strftime("%A")

def getHour(timestamp: Timestamp) -> Int:
    """
    >>> getHour(currentTimestamp)
    16
    """
    return timestamp.hour

def getMinute(timestamp: Timestamp) -> Int:
    """
    >>> getMinute(currentTimestamp)
    9
    """
    return timestamp.minute

def getSecond(timestamp: Timestamp) -> Int:
    """
    >>> getSecond(currentTimestamp)
    32
    """
    return timestamp.second
