import base64
import json
from typing import cast


## TODO: Narrow `None` out of the return type if possible.
def get_tenant_id_from_jwt(token: str) -> str:
    claims = extract_claims_from_jwt(token)

    return claims["sub"]


def get_addresses_from_jwt(token: str) -> tuple[str, str]:
    claims = extract_claims_from_jwt(token)

    return claims["server_url"], claims["grpc_broadcast_address"]


def extract_claims_from_jwt(token: str) -> dict[str, str]:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid token format")

    claims_part = parts[1]
    claims_part += "=" * ((4 - len(claims_part) % 4) % 4)  # Padding for base64 decoding
    claims_data = base64.urlsafe_b64decode(claims_part)

    return cast(dict[str, str], json.loads(claims_data))
