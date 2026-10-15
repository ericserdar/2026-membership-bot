"""Send admin email through Amazon SES (v2 API) with aiohttp — no boto3.

Replaced SendGrid in Oct 2026 (its free plan ended Sep 10 and every send
failed with 401 "Maximum credits exceeded"). The credentials belong to the
IAM user cougconnect-ses-bot, which may only send as cougconnect.com in
us-east-2. The prediction bot carries an identical copy of this file.

Env:
    SES_ACCESS_KEY_ID, SES_SECRET_ACCESS_KEY   required
    SES_REGION                                 default us-east-2
"""

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formataddr

import aiohttp



def _env() -> tuple[str, str, str]:
    """Read at call time, not import time, so it works whenever load_dotenv() runs."""
    return (os.getenv("SES_ACCESS_KEY_ID", ""), os.getenv("SES_SECRET_ACCESS_KEY", ""),
            os.getenv("SES_REGION", "") or "us-east-2")


class SESError(Exception):
    pass


def configured() -> bool:
    key_id, secret, _ = _env()
    return bool(key_id and secret)


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _auth_headers(method: str, path: str, body: bytes) -> dict:
    """SigV4 headers for one request to the SES v2 endpoint."""
    key_id, secret, region = _env()
    host = f"email.{region}.amazonaws.com"
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    scope = f"{date}/{region}/ses/aws4_request"

    headers = {"content-type": "application/json", "host": host, "x-amz-date": amz_date}
    signed = ";".join(headers)
    canonical = "\n".join([
        method, path, "",
        "".join(f"{k}:{v}\n" for k, v in headers.items()),
        signed,
        hashlib.sha256(body).hexdigest(),
    ])
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])

    key = _sign(("AWS4" + secret).encode(), date)
    for part in (region, "ses", "aws4_request"):
        key = _sign(key, part)
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()

    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={key_id}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature}"
    )
    del headers["host"]  # aiohttp sets it
    return headers


async def send_email(from_addr: str, from_name: str, to_addr: str, subject: str, body_text: str,
                     attachment_path: str | None = None, attachment_name: str | None = None) -> str:
    """Send one plain-text email, optionally with a file attached. Returns the SES MessageId.

    Raises SESError on any failure. SES caps a raw message at 40 MB including the
    base64 overhead, so attachments over ~29 MB will be refused.
    """
    if not configured():
        raise SESError("SES_ACCESS_KEY_ID / SES_SECRET_ACCESS_KEY not configured")

    msg = EmailMessage()
    msg["From"] = formataddr((from_name, from_addr))
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body_text)
    if attachment_path:
        with open(attachment_path, "rb") as f:
            msg.add_attachment(f.read(), maintype="application", subtype="octet-stream",
                               filename=attachment_name or os.path.basename(attachment_path))

    path = "/v2/email/outbound-emails"
    body = json.dumps({"Content": {"Raw": {"Data": base64.b64encode(msg.as_bytes()).decode()}}}).encode()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://email.{_env()[2]}.amazonaws.com{path}",
                data=body,
                headers=_auth_headers("POST", path, body),
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                text = await resp.text()
                if resp.status >= 300:
                    raise SESError(f"SES {resp.status} {resp.headers.get('x-amzn-ErrorType', '')}: {text[:300]}")
                return json.loads(text).get("MessageId", "")
    except aiohttp.ClientError as e:
        raise SESError(f"SES request failed: {e!r}") from e
