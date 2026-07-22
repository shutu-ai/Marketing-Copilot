from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from services import whitelist_service
import os
import uuid

router = APIRouter(prefix="/api/whitelist", tags=["Whitelist"])

UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "uploads")


@router.get("/history")
def get_history():
    return whitelist_service.get_upload_history()


@router.post("/preview")
async def preview(file: UploadFile = File(...)):
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    ext = os.path.splitext(file.filename)[1].lower()
    if ext != '.csv':
        raise HTTPException(status_code=400, detail="仅支持CSV文件")

    save_path = os.path.join(UPLOAD_DIR, f"_preview_{uuid.uuid4().hex}{ext}")
    content = await file.read()
    with open(save_path, "wb") as f:
        f.write(content)

    result = whitelist_service.preview_file(save_path)
    result["file_path"] = save_path
    return result


@router.post("/import")
async def import_file(
    file_path: str = Form(...),
    import_mode: str = Form("append"),
):
    from database import get_connection

    conn = get_connection()
    cursor = conn.execute(
        "INSERT INTO upload_history (filename, import_mode) VALUES (?, ?)",
        (os.path.basename(file_path), import_mode)
    )
    upload_id = cursor.lastrowid
    conn.commit()
    conn.close()

    result = whitelist_service.import_file(upload_id, file_path, import_mode)
    return result


@router.delete("/history/{history_id}")
def delete_history(history_id: int):
    from database import get_connection
    conn = get_connection()
    conn.execute("DELETE FROM whitelist_users WHERE upload_id=?", (history_id,))
    cursor = conn.execute("DELETE FROM upload_history WHERE id=?", (history_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    if not deleted:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"message": "删除成功"}


@router.get("/users")
def list_users(upload_id: int = None):
    from database import get_connection
    conn = get_connection()
    if upload_id:
        rows = conn.execute(
            "SELECT * FROM whitelist_users WHERE upload_id=?", (upload_id,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM whitelist_users ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]
