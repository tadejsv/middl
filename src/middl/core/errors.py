"""Module defining middl-specific errors."""

__all__ = ["AbortPipeline", "SkipStep", "ValidationError"]


class SkipStep(Exception):  # noqa: N818
    """
    Exception to skip processing of the current batch.

    When raised by a middleware, the pipeline skips the current step and
    continues with the next one.
    """


class AbortPipeline(Exception):  # noqa: N818
    """
    Exception to abort the entire pipeline execution.

    When raised by a middleware, the pipeline immediately exits.
    """


class ValidationError(Exception):
    """
    Exception raised when pipeline validation fails due to missing required fields.

    This exception indicates that the state or the batch from the data loader is missing
    fields required by one of the middlewares.
    """
