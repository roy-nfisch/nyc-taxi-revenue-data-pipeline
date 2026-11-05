"""Optional database-first ELT foundation; importing it opens no database.

DuckDB is installed through the elt extra. This package deliberately imports
neither DuckDB nor the ETL stages. Loading and transformation are separate tasks.
"""
