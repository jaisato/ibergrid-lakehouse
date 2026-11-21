"""OMIE's day-ahead marginal prices into ``bronze.omie_day_ahead_prices`` (ADR 0004).

``client`` downloads the daily ``marginalpdbc`` files, ``marginalpdbc`` holds their two contract
versions and turns a file into validated periods, and ``ingest`` lands the original files and
writes each chunk of delivery days through Write-Audit-Publish.
"""
