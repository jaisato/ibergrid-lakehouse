"""The S3 client of the readiness check and the landing zone: pyarrow's, path-style, own keys."""

from pyarrow import fs

from ibergrid.settings import StorageSettings, base_url


def s3_filesystem(
    storage: StorageSettings, *, timeout: float, max_attempts: int = 3
) -> fs.S3FileSystem:
    """``timeout`` bounds connecting and each request; ``max_attempts`` counts the first one."""
    return fs.S3FileSystem(
        access_key=storage.access_key.get_secret_value(),
        secret_key=storage.secret_key.get_secret_value(),
        region=storage.region,
        endpoint_override=base_url(storage.endpoint_url),
        force_virtual_addressing=False,
        connect_timeout=timeout,
        request_timeout=timeout,
        retry_strategy=fs.AwsStandardS3RetryStrategy(max_attempts=max_attempts),
    )
