def get_metadata(token: str | None) -> list[tuple[str, str]]:
    return [("authorization", "bearer " + (token or ""))]
