from dataclasses import dataclass
from app.db import db

@dataclass(frozen=True)
class MetricResult:
    value: int

class MetricsService:
    async def run_sql(self, sql: str) -> MetricResult:
        s = (sql or "").strip()
        s = s.removeprefix("```sql").removeprefix("```").strip()
        s = s.removesuffix("```").strip()
        s = s.split(";", 1)[0].strip()

        if not s.lower().startswith("select"):
            return MetricResult(0)

        await db.connect()
        async with db.acquire() as conn:
            v = await conn.fetchval(s)

        try:
            return MetricResult(int(v or 0))
        except Exception:
            return MetricResult(0)