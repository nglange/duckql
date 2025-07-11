"""Depth limiting extension for Strawberry GraphQL."""

from typing import Any, Dict, Optional
from strawberry.extensions import SchemaExtension
from strawberry.types import Info
from graphql import GraphQLError, FieldNode


class DepthLimitExtension(SchemaExtension):
    """
    Extension that limits the depth of GraphQL queries.
    
    This prevents deeply nested queries from consuming too many resources.
    """
    
    def __init__(self, *, max_depth: int = 10, ignore_introspection: bool = True):
        """
        Initialize the depth limit extension.
        
        Args:
            max_depth: Maximum allowed query depth
            ignore_introspection: Whether to ignore introspection queries
        """
        self.max_depth = max_depth
        self.ignore_introspection = ignore_introspection
    
    def on_operation(self) -> None:
        """Called at the start of the operation."""
        # Get the execution context
        execution_context = self.execution_context
        
        # Check if we have a document
        if not execution_context or not execution_context.query:
            return
        
        # Parse the query if needed
        from graphql import parse
        
        try:
            if isinstance(execution_context.query, str):
                document = parse(execution_context.query)
            else:
                document = execution_context.query
        except Exception:
            return
        
        # Check depth for each operation
        for definition in document.definitions:
            if hasattr(definition, 'selection_set') and definition.selection_set:
                for selection in definition.selection_set.selections:
                    if isinstance(selection, FieldNode):
                        # Skip introspection fields if configured
                        if self.ignore_introspection and selection.name.value.startswith('__'):
                            continue
                        
                        depth = self._calculate_depth(selection, 1)
                        
                        if depth > self.max_depth:
                            raise GraphQLError(
                                f"Query depth ({depth}) exceeds maximum allowed depth ({self.max_depth})",
                                nodes=[selection]
                            )
    
    def _calculate_depth(self, field: FieldNode, current_depth: int) -> int:
        """
        Calculate the depth of a field node.
        
        Args:
            field: The field node to calculate depth for
            current_depth: Current depth in the traversal
            
        Returns:
            Maximum depth found in this branch
        """
        if not field.selection_set:
            return current_depth
        
        max_depth = current_depth
        
        for selection in field.selection_set.selections:
            if isinstance(selection, FieldNode):
                # Regular field - recurse
                depth = self._calculate_depth(selection, current_depth + 1)
                max_depth = max(max_depth, depth)
        
        return max_depth


def create_depth_limit_extension(max_depth: int) -> type:
    """
    Create a depth limit extension class with specified max depth.
    
    Args:
        max_depth: Maximum allowed query depth
        
    Returns:
        DepthLimitExtension class configured with max_depth
    """
    class ConfiguredDepthLimitExtension(DepthLimitExtension):
        def __init__(self, **kwargs):
            # Ignore strawberry's execution_context kwarg
            super().__init__(max_depth=max_depth)
    
    return ConfiguredDepthLimitExtension