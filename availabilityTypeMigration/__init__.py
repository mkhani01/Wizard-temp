"""
Availability Types Migration – seeds availability_types table from CSV.
Runnable from the Migration Wizard or CLI: python main.py availability-types
"""

from .main import run

__all__ = ["run"]
