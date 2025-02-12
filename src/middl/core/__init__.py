"""
Module containing core objects for implementing pipelines and creating
concrete middleware subclasses.
"""

from .errors import AbortPipeline, SkipStep, ValidationError
from .loader import (
    EmptyLoader,
    Loader,
    WrappedSizedLoader,
    WrappedUnsizedLoader,
    wrap_iterable,
)
from .middleware import Middleware, ProcessingStep, StrMapping
from .pipeline import Pipeline, PipelineWrapper

__all__ = [
    "AbortPipeline",
    "EmptyLoader",
    "Loader",
    "Middleware",
    "Pipeline",
    "PipelineWrapper",
    "ProcessingStep",
    "SkipStep",
    "StrMapping",
    "ValidationError",
    "WrappedSizedLoader",
    "WrappedUnsizedLoader",
    "wrap_iterable",
]
