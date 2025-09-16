import abc
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class Plot(abc.ABC):
    title: str = None
    metadata: Dict[str, Any] = Dict[str, Any]


@dataclass
class XYPlot(Plot):
    x_data: Any = None
    x_label: str = None
    y_data: Any = None
    y_label: str = None


@dataclass
class SequencePlot(Plot):
    data: Any = None
    label: str = None
