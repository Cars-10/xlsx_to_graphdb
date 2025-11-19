"""
Custom exceptions for the Windchill XLSX to Knowledge Graph importer.
Provides specific error types for different failure scenarios.
"""


class WindchillImporterError(Exception):
    """Base exception for all Windchill importer errors."""
    pass


class ValidationError(WindchillImporterError):
    """Raised when input validation fails."""
    def __init__(self, message: str, field: str = None, value: str = None):
        super().__init__(message)
        self.field = field
        self.value = value


class FileValidationError(ValidationError):
    """Raised when file validation fails."""
    pass


class ExcelValidationError(FileValidationError):
    """Raised when Excel file validation fails."""
    pass


class CSVValidationError(FileValidationError):
    """Raised when CSV file validation fails."""
    pass


class DatabaseConnectionError(WindchillImporterError):
    """Raised when database connection fails."""
    def __init__(self, message: str, database_type: str = None, url: str = None):
        super().__init__(message)
        self.database_type = database_type
        self.url = url


class DatabaseQueryError(WindchillImporterError):
    """Raised when database query execution fails."""
    def __init__(self, message: str, query: str = None, database_type: str = None):
        super().__init__(message)
        self.query = query
        self.database_type = database_type


class ConfigurationError(WindchillImporterError):
    """Raised when configuration is invalid."""
    def __init__(self, message: str, field: str = None, value: str = None):
        super().__init__(message)
        self.field = field
        self.value = value


class DataProcessingError(WindchillImporterError):
    """Raised when data processing fails."""
    def __init__(self, message: str, row: int = None, sheet: str = None):
        super().__init__(message)
        self.row = row
        self.sheet = sheet


class NameResolutionError(DataProcessingError):
    """Raised when name resolution fails during BOM processing."""
    pass


class NetworkError(WindchillImporterError):
    """Raised when network operations fail."""
    def __init__(self, message: str, url: str = None, status_code: int = None):
        super().__init__(message)
        self.url = url
        self.status_code = status_code


class TimeoutError(NetworkError):
    """Raised when network operations timeout."""
    pass


class AuthenticationError(NetworkError):
    """Raised when authentication fails."""
    pass


class RateLimitError(NetworkError):
    """Raised when rate limiting is encountered."""
    pass