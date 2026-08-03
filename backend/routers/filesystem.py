import os
import string

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/filesystem", tags=["Filesystem"])


def _roots() -> list[str]:
    if os.name != "nt":
        return [os.path.sep]
    return [f"{drive}:\\" for drive in string.ascii_uppercase if os.path.exists(f"{drive}:\\")]


@router.get("/directories")
def list_directories(path: str = Query("")):
    """List directories visible to the backend service account.

    This endpoint is intentionally unrestricted by root path so administrators
    can select any writable campaign location on either Windows or Linux.
    """
    if not path:
        return {
            "path": "",
            "parent": None,
            "entries": [{"name": root, "path": root} for root in _roots()],
        }

    current = os.path.abspath(os.path.expanduser(path))
    if not os.path.isdir(current):
        raise HTTPException(status_code=404, detail="Directory does not exist or is not accessible")
    try:
        entries = [
            {"name": name, "path": os.path.join(current, name)}
            for name in os.listdir(current)
            if os.path.isdir(os.path.join(current, name))
        ]
    except OSError as exc:
        raise HTTPException(status_code=403, detail=f"Cannot read directory: {exc}")

    parent = os.path.dirname(current)
    if parent == current:
        parent = None
    return {"path": current, "parent": parent, "entries": sorted(entries, key=lambda item: item["name"].lower())}
