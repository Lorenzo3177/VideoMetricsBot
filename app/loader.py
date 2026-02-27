import asyncio
import json
from datetime import datetime
from typing import Any

from app.config import settings
from app.db import db


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


async def load_data(batch_size: int = 1000) -> None:
    with open(settings.data_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    videos: list[dict[str, Any]] = payload["videos"]

    await db.connect()

    async with db.acquire() as conn:
        await conn.execute("TRUNCATE TABLE video_snapshots, videos CASCADE;")

        video_rows = []
        snapshot_rows = []

        for v in videos:
            video_rows.append((
                v["id"],
                v["creator_id"],
                parse_ts(v["video_created_at"]),
                v["views_count"],
                v["likes_count"],
                v["comments_count"],
                v["reports_count"],
                parse_ts(v["created_at"]),
                parse_ts(v["updated_at"]),
            ))

            for s in v.get("snapshots", []):
                snapshot_rows.append((
                    s["id"],
                    s["video_id"],
                    s["views_count"],
                    s["likes_count"],
                    s["comments_count"],
                    s["reports_count"],
                    s["delta_views_count"],
                    s["delta_likes_count"],
                    s["delta_comments_count"],
                    s["delta_reports_count"],
                    parse_ts(s["created_at"]),
                    parse_ts(s["updated_at"]),
                ))

        await conn.executemany(
            """
            INSERT INTO videos(
                id, creator_id, video_created_at,
                views_count, likes_count, comments_count, reports_count,
                created_at, updated_at
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            video_rows,
        )

        insert_snap = """
            INSERT INTO video_snapshots(
                id, video_id,
                views_count, likes_count, comments_count, reports_count,
                delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                created_at, updated_at
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        """

        for i in range(0, len(snapshot_rows), batch_size):
            part = snapshot_rows[i:i + batch_size]
            await conn.executemany(insert_snap, part)

    await db.close()
    print(f"Loaded: videos={len(video_rows)}, snapshots={len(snapshot_rows)}")


if __name__ == "__main__":
    asyncio.run(load_data())