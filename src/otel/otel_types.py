from typing import Dict, Literal, Optional

MetricType = Literal["counter", "gauge", "histogram"]


class Metric:
    def __init__(
        self,
        metric_type: MetricType,
        name: str,
        value: int | float,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ):
        self.type = metric_type
        self.name = name
        self.value = value
        self.attributes = attributes or {}
        self.description = description
        self.unit = unit

    @classmethod
    def counter(cls, name: str, value: int = 1, attributes: Optional[Dict[str, str]] = None, description: str = "", unit: str = "1") -> "Metric":
        """Create a counter metric"""
        return cls("counter", name, value, attributes, description, unit)

    @classmethod
    def gauge(cls, name: str, value: float, attributes: Optional[Dict[str, str]] = None, description: str = "", unit: str = "1") -> "Metric":
        """Create a gauge metric"""
        return cls("gauge", name, value, attributes, description, unit)

    @classmethod
    def histogram(cls, name: str, value: float, attributes: Optional[Dict[str, str]] = None, description: str = "", unit: str = "1") -> "Metric":
        """Create a histogram metric"""
        return cls("histogram", name, value, attributes, description, unit)
