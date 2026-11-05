"""Keep the existing Prefect command; the ETL flow lives in green_taxi.etl.flow.

This delegate has no orchestration or data rules. Importing it does not run the
flow; executing python -m green_taxi.flow invokes the existing Click command.
"""

from green_taxi.etl.flow import main as main
from green_taxi.etl.flow import taxi_flow as taxi_flow

if __name__ == "__main__":
    main()
