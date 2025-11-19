"""
Logging configuration for the Windchill XLSX to Knowledge Graph importer.
Provides structured logging with consistent formatting and log levels.
"""

import logging
import logging.handlers
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any


class StructuredFormatter(logging.Formatter):
    """Custom formatter that can output structured logs in JSON format."""
    
    def __init__(self, structured: bool = False):
        super().__init__()
        self.structured = structured
    
    def format(self, record: logging.LogRecord) -> str:
        if self.structured:
            return self._format_structured(record)
        else:
            return self._format_text(record)
    
    def _format_structured(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ('name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'exc_info', 'exc_text', 'stack_info', 
                          'lineno', 'funcName', 'created', 'msecs', 'relativeCreated', 
                          'thread', 'threadName', 'processName', 'process', 'getMessage'):
                log_entry[key] = value
        
        return json.dumps(log_entry)
    
    def _format_text(self, record: logging.LogRecord) -> str:
        """Format log record as human-readable text."""
        return super().format(record)


def setup_logging(
    level: str = 'INFO',
    log_file: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    structured: bool = False,
    include_console: bool = True
) -> None:
    """
    Set up logging configuration with consistent formatting.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        max_file_size: Maximum size of log file before rotation
        backup_count: Number of backup files to keep
        structured: Whether to use JSON structured logging
        include_console: Whether to include console output
    """
    
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    if structured:
        formatter = StructuredFormatter(structured=True)
    else:
        formatter = StructuredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Console handler
    if include_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler with rotation
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Set specific levels for noisy libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('openpyxl').setLevel(logging.WARNING)
    
    # Log the logging setup (meta!)
    logger.info(f"Logging configured: level={level}, file={log_file}, structured={structured}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


class LogContext:
    """Context manager for adding contextual information to logs."""
    
    def __init__(self, **kwargs):
        self.context = kwargs
    
    def __enter__(self):
        # Store original context
        self.original_context = getattr(logging.getLogger(), '_context', {})
        logging.getLogger()._context = self.context.copy()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original context
        logging.getLogger()._context = self.original_context


def log_operation_start(operation: str, **kwargs) -> None:
    """Log the start of an operation with context."""
    logger = get_logger(__name__)
    logger.info(f"Starting operation: {operation}", extra={
        'operation': operation,
        'status': 'started',
        'context': kwargs
    })


def log_operation_end(operation: str, success: bool = True, duration: Optional[float] = None, **kwargs) -> None:
    """Log the end of an operation with result."""
    logger = get_logger(__name__)
    status = 'completed' if success else 'failed'
    message = f"Operation {operation} {status}"
    
    if duration is not None:
        message += f" (duration: {duration:.2f}s)"
    
    extra_data = {
        'operation': operation,
        'status': status,
        'success': success,
        'context': kwargs
    }
    
    if duration is not None:
        extra_data['duration'] = duration
    
    if success:
        logger.info(message, extra=extra_data)
    else:
        logger.error(message, extra=extra_data)


def log_validation_error(error: Exception, field: str = None, value: str = None) -> None:
    """Log validation errors with context."""
    logger = get_logger(__name__)
    extra_data = {
        'error_type': type(error).__name__,
        'field': field,
        'value': value
    }
    logger.warning(f"Validation error: {str(error)}", extra=extra_data)


def log_database_operation(operation: str, database_type: str, success: bool = True, **kwargs) -> None:
    """Log database operations with context."""
    logger = get_logger(__name__)
    status = 'succeeded' if success else 'failed'
    message = f"Database operation '{operation}' {status} on {database_type}"
    
    extra_data = {
        'operation': operation,
        'database_type': database_type,
        'success': success,
        'context': kwargs
    }
    
    if success:
        logger.info(message, extra=extra_data)
    else:
        logger.error(message, extra=extra_data)