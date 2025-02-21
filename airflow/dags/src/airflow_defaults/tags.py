class Period:
    """
    Период запуска.
    """
    once = "once"
    half_hour = "half-hour"
    hourly = "hourly"
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"


class OwnerGroup:
    """
    Группа владельцев/поддержки
    """
    analytics = "analytics"
    engineer = "engineer"


class ProcessType:
    test = "test"
    critical = "critical"
    service = "service"