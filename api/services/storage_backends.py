"""
Storage backend abstraction for PDF brochure files.

Provides a uniform interface (StorageBackend protocol) over:
  - LocalBackend  — local filesystem (default, backward-compatible)
  - S3Backend     — Amazon S3 or any S3-compatible store
  - AzureBackend  — Azure Blob Storage

URI scheme used in adv_brochures.file_path:
  - Local (legacy): absolute path  /data/brochures/{crd}/{vid}_{date}.pdf
  - S3:             s3://{bucket}/brochures/{crd}/{vid}_{date}.pdf
  - Azure:          azure://{container}/brochures/{crd}/{vid}_{date}.pdf
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Protocol, runtime_checkable

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class StorageBackend(Protocol):
    def put(self, key: str, data: bytes) -> None: ...
    def get(self, key: str) -> bytes: ...
    def stream(self, key: str, chunk_size: int = 65536) -> Iterator[bytes]: ...
    def exists(self, key: str) -> bool: ...
    def delete(self, key: str) -> None: ...
    def uri_for(self, key: str) -> str: ...


# ---------------------------------------------------------------------------
# Local backend
# ---------------------------------------------------------------------------

class LocalBackend:
    def __init__(self, base_dir: Path) -> None:
        self._base = base_dir

    def put(self, key: str, data: bytes) -> None:
        dest = self._base / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)

    def get(self, key: str) -> bytes:
        return (self._base / key).read_bytes()

    def stream(self, key: str, chunk_size: int = 65536) -> Iterator[bytes]:
        path = self._base / key
        with open(path, "rb") as fh:
            while chunk := fh.read(chunk_size):
                yield chunk

    def exists(self, key: str) -> bool:
        return (self._base / key).exists()

    def delete(self, key: str) -> None:
        p = self._base / key
        if p.exists():
            p.unlink()

    def uri_for(self, key: str) -> str:
        # Returns absolute path — fully backward-compatible with existing file_path values
        return str(self._base / key)


# ---------------------------------------------------------------------------
# S3 backend
# ---------------------------------------------------------------------------

class S3Backend:
    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        import boto3  # imported here so local-only deployments don't need the package loaded

        kwargs: dict = {}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if access_key_id and secret_access_key:
            kwargs["aws_access_key_id"] = access_key_id
            kwargs["aws_secret_access_key"] = secret_access_key
        if region:
            kwargs["region_name"] = region

        self._client = boto3.client("s3", **kwargs)
        self._bucket = bucket

    def put(self, key: str, data: bytes) -> None:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType="application/pdf",
        )

    def get(self, key: str) -> bytes:
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        return resp["Body"].read()

    def stream(self, key: str, chunk_size: int = 65536) -> Iterator[bytes]:
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        body = resp["Body"]
        while chunk := body.read(chunk_size):
            yield chunk

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError:
            return False

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=key)

    def uri_for(self, key: str) -> str:
        return f"s3://{self._bucket}/{key}"


# ---------------------------------------------------------------------------
# Azure backend
# ---------------------------------------------------------------------------

class AzureBackend:
    def __init__(self, container: str, connection_string: str) -> None:
        from azure.storage.blob import BlobServiceClient  # imported here

        self._container = container
        self._cc = BlobServiceClient.from_connection_string(connection_string).get_container_client(container)

    def put(self, key: str, data: bytes) -> None:
        from azure.storage.blob import ContentSettings

        self._cc.upload_blob(
            key,
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/pdf"),
        )

    def get(self, key: str) -> bytes:
        return self._cc.download_blob(key).readall()

    def stream(self, key: str, chunk_size: int = 65536) -> Iterator[bytes]:
        yield from self._cc.download_blob(key).chunks()

    def exists(self, key: str) -> bool:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self._cc.get_blob_properties(key)
            return True
        except ResourceNotFoundError:
            return False

    def delete(self, key: str) -> None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self._cc.delete_blob(key)
        except ResourceNotFoundError:
            pass

    def uri_for(self, key: str) -> str:
        return f"azure://{self._container}/{key}"


# ---------------------------------------------------------------------------
# URI helpers
# ---------------------------------------------------------------------------

def key_from_uri(uri: str) -> tuple[str, str]:
    """
    Parse a storage URI and return (scheme, key).

    scheme is 'local', 's3', or 'azure'.
    For legacy absolute paths (starts with '/'), scheme is 'local' and
    key is the full absolute path (LocalBackend handles it transparently).
    """
    if uri.startswith("s3://"):
        # s3://bucket/brochures/100001/12345678_20250301.pdf
        rest = uri[5:]  # "bucket/brochures/..."
        _, key = rest.split("/", 1)
        return "s3", key
    if uri.startswith("azure://"):
        # azure://container/brochures/100001/12345678_20250301.pdf
        rest = uri[8:]  # "container/brochures/..."
        _, key = rest.split("/", 1)
        return "azure", key
    # Legacy absolute path — pass as-is as the "key" for LocalBackend
    return "local", uri


def make_brochure_key(crd: int, version_id: int, date_tag: str) -> str:
    """Canonical object key used by all backends (relative path, no leading slash)."""
    return f"brochures/{crd}/{version_id}_{date_tag}.pdf"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_active_backend(db) -> StorageBackend:  # type: ignore[type-arg]
    """
    Read StorageSettings from DB (id=1) and return the appropriate backend.
    Falls back to LocalBackend if the row is missing.
    """
    from config import settings as app_settings
    from models.storage_settings import StorageSettings

    row: StorageSettings | None = db.get(StorageSettings, 1)

    if row is None or row.backend == "local":
        return LocalBackend(Path(app_settings.data_dir))

    if row.backend == "s3":
        return S3Backend(
            bucket=row.s3_bucket or "",
            region=row.s3_region,
            access_key_id=row.s3_access_key_id,
            secret_access_key=row.s3_secret_access_key,
            endpoint_url=row.s3_endpoint_url,
        )

    if row.backend == "azure":
        return AzureBackend(
            container=row.azure_container or "",
            connection_string=row.azure_connection_string or "",
        )

    raise ValueError(f"Unknown storage backend: {row.backend!r}")
