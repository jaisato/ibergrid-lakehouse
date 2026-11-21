"""Red Eléctrica's REData API into ``bronze.redata_observations`` (ADR 0003).

``datasets`` lists the widgets and how they are asked for, ``client`` talks to the API,
``normalize`` turns answers into validated rows and measures completeness, and ``ingest`` lands
the raw answers and writes each chunk through Write-Audit-Publish.
"""
