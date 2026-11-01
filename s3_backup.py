"""Upload a database snapshot to Amazon S3 with aiohttp — no boto3.

Nightly off-Railway copy of the bot's SQLite DB (added Nov 2026; the email
backup moved to weekly). Uses the same IAM user as ses_mail.py
(cougconnect-ses-bot), which may only PutObject into the backup bucket — it
cannot read, list or delete. The bucket expires objects after 30 days.
The prediction bot carries an identical copy of this file.

Env:
    BACKUP_S3_BUCKET                           unset = upload skipped
    SES_ACCESS_KEY_ID, SES_SECRET_ACCESS_KEY   required (shared with ses_mail)
    SES_REGION                                 default us-east-2
"""

import hashlib
import hmac
import os
from datetime import datetime, timezone

import aiohttp


class S3Error(Exception):
    pass


def _env() -> tuple[str, str, str, str]:
    """Read at call time, not import time, so it works whenever load_dotenv() runs."""
    return (os.getenv("BACKUP_S3_BUCKET", ""), os.getenv("SES_ACCESS_KEY_ID", ""),
            os.getenv("SES_SECRET_ACCESS_KEY", ""), os.getenv("SES_REGION", "") or "us-east-2")


def configured() -> bool:
    bucket, key_id, secret, _ = _env()
    return bool(bucket and key_id and secret)


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _auth_headers(host: str, path: str, body: bytes) -> dict:
    """SigV4 headers for one S3 PUT."""
    _, key_id, secret, region = _env()
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    scope = f"{date}/{region}/s3/aws4_request"
    payload_hash = hashlib.sha256(body).hexdigest()

    headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
    signed = ";".join(headers)
    canonical = "\n".join([
        "PUT", path, "",
        "".join(f"{k}:{v}\n" for k, v in headers.items()),
        signed,
        payload_hash,
    ])
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])

    key = _sign(("AWS4" + secret).encode(), date)
    for part in (region, "s3", "aws4_request"):
        key = _sign(key, part)
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()

    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={key_id}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature}"
    )
    del headers["host"]  # aiohttp sets it
    return headers


async def upload(file_path: str, object_key: str) -> str:
    """PUT one file to s3://$BACKUP_S3_BUCKET/<object_key>. Returns the s3:// URI.

    object_key must be plain ASCII path segments (letters, digits, - _ . /).
    Raises S3Error on any failure.
    """
    if not configured():
        raise S3Error("BACKUP_S3_BUCKET / SES_ACCESS_KEY_ID / SES_SECRET_ACCESS_KEY not configured")
    bucket, _, _, region = _env()
    host = f"{bucket}.s3.{region}.amazonaws.com"
    path = "/" + object_key.lstrip("/")
    with open(file_path, "rb") as f:
        body = f.read()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"https://{host}{path}",
                data=body,
                headers=_auth_headers(host, path, body),
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status >= 300:
                    text = await resp.text()
                    raise S3Error(f"S3 {resp.status}: {text[:300]}")
    except aiohttp.ClientError as e:
        raise S3Error(f"S3 request failed: {e!r}") from e
    return f"s3://{bucket}{path}"
