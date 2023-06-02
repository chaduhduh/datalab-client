"""
Provides various useful tools to interface with async jobs
"""
from dl import queryClient


class Status:
    """
    Various states of an async job
    """
    COMPLETED = "COMPLETED"
    EXECUTING = "EXECUTING"
    ERROR = "ERROR"
    ABORTED = "ABORTED"


class Job:
    """
    Interface with a long running async job
    """
    def __init__(self, id: str=None, qc: queryClient=None) -> None:
        self.id = id
        self.qc = qc

    def status_text(self):
        """ returns the current status text of the job """
        return self.qc.status(token=None, jobId=self.id)

    def done(self):
        """ returns True if the job is finished running """
        return self.status_text() != Status.EXECUTING

    def error(self):
        """ returns True if the job finished with an error """
        return self.status_text() == Status.ERROR

    def success(self):
        """ returns True if the job completed succesfully """
        return self.status_text() == Status.COMPLETED

    def results(self):
        """ shortcut to return the job output data """
        return self.qc.results(token=None, jobId=self.id)

    def abort(self):
        """ attempt to abort the current job """
        return self.qc.abort(token=None, jobId=self.id)
