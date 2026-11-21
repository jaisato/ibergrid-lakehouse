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


def land(
    filesystem: fs.FileSystem, bucket: str, key: str, payload: bytes, *, content_type: str
) -> str:
    """Write a source's raw answer at ``<bucket>/<key>``, replacing any earlier one; its URI."""
    path = f"{bucket}/{key}"
    if filesystem.type_name != "s3":  # a local disk (tests, a laptop) needs the directories
        filesystem.create_dir(path.rpartition("/")[0], recursive=True)
    with filesystem.open_output_stream(path, metadata={"Content-Type": content_type}) as out:
        out.write(payload)
    return f"s3://{path}"
