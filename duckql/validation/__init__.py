"""GraphQL query validation utilities."""

from .depth_extension import DepthLimitExtension, create_depth_limit_extension

__all__ = ['DepthLimitExtension', 'create_depth_limit_extension']