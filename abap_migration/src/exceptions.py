"""
Custom exceptions for ETL processes
"""


class ETLBaseException(Exception):
    """Base exception for ETL operations"""
    pass


class ETLExtractionError(ETLBaseException):
    """Raised when extraction fails"""
    pass


class ETLTransformationError(ETLBaseException):
    """Raised when transformation fails"""
    pass


class ETLLoadError(ETLBaseException):
    """Raised when loading fails"""
    pass


class ETLValidationError(ETLBaseException):
    """Raised when validation fails"""
    pass


class ETLOrchestrationError(ETLBaseException):
    """Raised when orchestration fails"""
    pass