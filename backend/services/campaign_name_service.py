from database import read_transaction


def ensure_unique_name(name: str, exclude_type: str = "", exclude_id: int = 0) -> str:
    normalized = (name or "").strip()
    if not normalized:
        raise ValueError("任务名称不能为空")
    rows = read_transaction(
        lambda conn: conn.execute(
            """SELECT 'batch' AS campaign_type, id FROM batch_campaigns WHERE name=?
               UNION ALL
               SELECT 'realtime' AS campaign_type, id FROM realtime_campaigns WHERE name=?""",
            (normalized, normalized),
        ).fetchall(),
        operation="campaign.name.check",
    )
    if any(row["campaign_type"] != exclude_type or row["id"] != exclude_id for row in rows):
        raise ValueError(f"任务名称已存在：{normalized}")
    return normalized
