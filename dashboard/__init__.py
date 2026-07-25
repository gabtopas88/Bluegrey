"""
Bluegrey — read-only telemetry dashboard package.

This package renders the engine's append-only Parquet telemetry as a Streamlit
web dashboard. It is strictly READ-ONLY with respect to the trading system:

  - It never connects to IBKR, so it can never disturb the single allowed
    market-data session the live engine holds, nor affect strategy behaviour.
  - It never writes telemetry. It imports only the schema *constants* from
    ``src.telemetry`` (which are side-effect-free) and mirrors the resilient
    read pattern; it does NOT instantiate ``TelemetryStore``, whose ``__init__``
    creates directories and writes a session manifest.

Modules:
  telemetry_reader : resilient, read-only access layer over the Parquet tree.
  utils            : timezone conversion + display formatting helpers.
  app              : the Streamlit entrypoint (run via ``streamlit run``).
"""