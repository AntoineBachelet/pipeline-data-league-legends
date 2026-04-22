def safe_name(name: str) -> str:
    """Sanitize a player name for use in S3 keys and file paths."""
    return name.replace(" ", "_").replace("/", "-")
