"""DuckQL - GraphQL interface for DuckDB databases."""

from .core import DuckQL
from .notebook import NotebookDuckQL, connect

__version__ = "0.1.0"
__all__ = ["DuckQL", "NotebookDuckQL", "connect"]