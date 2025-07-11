"""Custom exceptions for DuckQL with enhanced error messages."""

from typing import Optional, Dict, Any, List
import uuid


class DuckQLError(Exception):
    """Base exception for all DuckQL errors."""
    
    def __init__(
        self, 
        message: str, 
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        suggestions: Optional[List[str]] = None,
        correlation_id: Optional[str] = None
    ):
        """
        Initialize DuckQL error with rich context.
        
        Args:
            message: The error message
            error_code: Optional error code for categorization
            context: Additional context about the error
            suggestions: List of suggestions to fix the error
            correlation_id: ID to track this error across logs
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "DUCKQL_ERROR"
        self.context = context or {}
        self.suggestions = suggestions or []
        self.correlation_id = correlation_id or str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for JSON serialization."""
        return {
            "error": self.error_code,
            "message": self.message,
            "context": self.context,
            "suggestions": self.suggestions,
            "correlation_id": self.correlation_id
        }
    
    def __str__(self) -> str:
        """Return formatted error message."""
        parts = [f"[{self.error_code}] {self.message}"]
        
        if self.context:
            parts.append(f"Context: {self.context}")
        
        if self.suggestions:
            parts.append("Suggestions:")
            for i, suggestion in enumerate(self.suggestions, 1):
                parts.append(f"  {i}. {suggestion}")
        
        parts.append(f"Correlation ID: {self.correlation_id}")
        
        return "\n".join(parts)


class SchemaError(DuckQLError):
    """Error during schema introspection or type generation."""
    
    def __init__(
        self, 
        message: str, 
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        **kwargs
    ):
        context = kwargs.pop("context", {})
        if table_name:
            context["table"] = table_name
        if column_name:
            context["column"] = column_name
        
        super().__init__(
            message=message,
            error_code="SCHEMA_ERROR",
            context=context,
            **kwargs
        )


class QueryError(DuckQLError):
    """Error during query execution."""
    
    def __init__(
        self, 
        message: str, 
        query: Optional[str] = None,
        table_name: Optional[str] = None,
        operation: Optional[str] = None,
        **kwargs
    ):
        context = kwargs.pop("context", {})
        if query:
            # Truncate long queries
            context["query"] = query[:200] + "..." if len(query) > 200 else query
        if table_name:
            context["table"] = table_name
        if operation:
            context["operation"] = operation
        
        super().__init__(
            message=message,
            error_code="QUERY_ERROR",
            context=context,
            **kwargs
        )


class ConnectionError(DuckQLError):
    """Database connection error."""
    
    def __init__(
        self, 
        message: str, 
        database_path: Optional[str] = None,
        **kwargs
    ):
        context = kwargs.pop("context", {})
        if database_path:
            context["database"] = database_path
        
        suggestions = kwargs.pop("suggestions", [])
        if not suggestions:
            suggestions = [
                "Check if the database file exists and is accessible",
                "Verify you have the necessary permissions",
                "Ensure the database is not locked by another process"
            ]
        
        super().__init__(
            message=message,
            error_code="CONNECTION_ERROR",
            context=context,
            suggestions=suggestions,
            **kwargs
        )


class ValidationError(DuckQLError):
    """GraphQL validation error."""
    
    def __init__(
        self, 
        message: str, 
        field_name: Optional[str] = None,
        expected_type: Optional[str] = None,
        actual_value: Optional[Any] = None,
        **kwargs
    ):
        context = kwargs.pop("context", {})
        if field_name:
            context["field"] = field_name
        if expected_type:
            context["expected_type"] = expected_type
        if actual_value is not None:
            context["actual_value"] = str(actual_value)
            context["actual_type"] = type(actual_value).__name__
        
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            context=context,
            **kwargs
        )


class FilterError(QueryError):
    """Error in WHERE clause or filter conditions."""
    
    def __init__(
        self, 
        message: str, 
        filter_field: Optional[str] = None,
        filter_operation: Optional[str] = None,
        **kwargs
    ):
        context = kwargs.pop("context", {})
        if filter_field:
            context["filter_field"] = filter_field
        if filter_operation:
            context["filter_operation"] = filter_operation
        
        # Remove error_code if present in kwargs to avoid duplicate
        kwargs.pop("error_code", None)
        
        super().__init__(
            message=message,
            context=context,
            **kwargs
        )


def enhance_duckdb_error(original_error: Exception, **context) -> DuckQLError:
    """
    Transform a DuckDB exception into a user-friendly DuckQL error.
    
    Args:
        original_error: The original DuckDB exception
        **context: Additional context to include
    
    Returns:
        Enhanced DuckQL error with helpful information
    """
    error_message = str(original_error)
    error_type = type(original_error).__name__
    
    # Column not found errors
    if "Could not find column" in error_message:
        # Try to extract column and table names
        import re
        
        # Pattern: "Could not find column 'username' in table 'users'"
        pattern = r"column\s*['\"]?(\w+)['\"]?\s*(?:in\s*table\s*['\"]?(\w+)['\"]?)?"
        match = re.search(pattern, error_message, re.IGNORECASE)
        
        if match:
            column_name = match.group(1)
            table_name = match.group(2) if match.group(2) else context.get("table", "unknown")
        else:
            column_name = "unknown"
            table_name = context.get("table", "unknown")
        
        return SchemaError(
            f"Column '{column_name}' not found",
            table_name=table_name,
            column_name=column_name,
            suggestions=[
                f"Check if the column name is spelled correctly",
                f"Use 'duckql tables {context.get('database', 'your.db')}' to see available columns",
                f"Ensure the column exists in table '{table_name}'"
            ]
        )
    
    # Type mismatch errors
    elif "Cannot compare values of type" in error_message or "Type mismatch" in error_message:
        return FilterError(
            "Type mismatch in filter condition",
            suggestions=[
                "Ensure you're comparing compatible types (numbers with numbers, strings with strings)",
                "Use proper quotes for string values",
                "Check if numeric fields are being compared with string values"
            ],
            context={"original_error": error_message}
        )
    
    # Syntax errors
    elif "Parser Error" in error_message or "Syntax error" in error_message:
        return QueryError(
            "SQL syntax error in generated query",
            suggestions=[
                "This might be a bug in DuckQL's SQL generation",
                "Try simplifying your GraphQL query",
                "Report this issue with your GraphQL query and schema"
            ],
            context={"original_error": error_message, **context}
        )
    
    # Connection errors
    elif error_type in ["ConnectionException", "IOException"]:
        return ConnectionError(
            "Database connection failed",
            context={"original_error": error_message, **context}
        )
    
    # Catalog errors (table not found)
    elif "Catalog Error" in error_message:
        import re
        # Pattern: "Table with name 'products' does not exist"
        table_match = re.search(r"Table\s*(?:with\s*name\s*)?['\"]?(\w+)['\"]?", error_message, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else "unknown"
        
        return SchemaError(
            f"Table '{table_name}' not found",
            table_name=table_name,
            suggestions=[
                f"Check if the table name is spelled correctly",
                f"Use 'duckql tables {context.get('database', 'your.db')}' to see available tables",
                f"Ensure the table has been created in the database"
            ]
        )
    
    # Generic fallback
    else:
        return QueryError(
            f"Database error: {error_message}",
            context={"error_type": error_type, **context}
        )