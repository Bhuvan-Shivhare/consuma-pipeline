"""Pipeline control-flow exceptions."""


class TransientError(Exception):
    """A retryable failure (simulated 500, poison pill, semaphore busy).
    The worker retries with exponential backoff, then dead-letters."""


class DuplicateStage(Exception):
    """This (job, stage) has already been committed. The worker acks and skips
    so duplicate deliveries and reaper re-emissions are no-ops."""
