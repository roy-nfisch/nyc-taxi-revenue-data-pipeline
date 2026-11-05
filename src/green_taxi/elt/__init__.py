"""Optional database-first ELT; importing this package opens no database.

DuckDB is installed through the elt extra. This package deliberately imports
neither DuckDB nor the ETL stages. Loading and transformation are separate
responsibilities.
"""
