from typing import Dict, Literal, Optional

MetricType = Literal["counter", "gauge", "histogram"]


# Base Metric class
class Metric:
    def __init__(
        self,
        name: str,
        value: int | float,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ):
        self.name = name
        self.value = value
        self.attributes = attributes or {}
        self.description = description
        self.unit = unit


class CounterMetric(Metric):
    type: MetricType = "counter"

    def __init__(
        self,
        name: str,
        value: int = 1,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ):
        super().__init__(name, value, attributes, description, unit)
        self.type = "counter"

    @classmethod
    def create(
        cls,
        name: str,
        value: int = 1,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ) -> "CounterMetric":
        return cls(name, value, attributes, description, unit)


class GaugeMetric(Metric):
    type: MetricType = "gauge"

    def __init__(
        self,
        name: str,
        value: float = 0.0,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ):
        super().__init__(name, value, attributes, description, unit)
        self.type = "gauge"

    @classmethod
    def create(
        cls,
        name: str,
        value: float,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ) -> "GaugeMetric":
        return cls(name, value, attributes, description, unit)


class HistogramMetric(Metric):
    type: MetricType = "histogram"

    def __init__(
        self,
        name: str,
        value: float,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
        boundaries: Optional[list[float]] = None,
    ):
        super().__init__(name, value, attributes, description, unit)
        self.type = "histogram"
        self.boundaries = boundaries

    @classmethod
    def create(
        cls,
        name: str,
        value: float,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
        boundaries: Optional[list[float]] = None,
    ) -> "HistogramMetric":
        return cls(name, value, attributes, description, unit, boundaries)
