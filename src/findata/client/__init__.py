"""
Client API for accessing financial data from the FinData database.

Provides a simple, intuitive interface for external projects to query data
without direct database access.
"""

from .client import DataClient

__all__ = ['DataClient']
