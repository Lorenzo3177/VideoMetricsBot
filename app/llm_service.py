import os
import time
import uuid
from datetime import datetime

import aiohttp

OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
CHAT_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

AUTH_KEY = os.getenv("GIGACHAT_AUTH_KEY")
SCOPE = os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")


def build_system_prompt() -> str:
    current_date = datetime.now().strftime("%Y-%m-%d")
    return f"""
Ты переводишь вопросы пользователя в SQL-запросы для PostgreSQL.

Верни ТОЛЬКО один SQL-запрос. Без пояснений, без markdown, без лишних символов.
Запрос обязан вернуть ОДНО число (одна строка, один столбец).
Разрешён ТОЛЬКО SELECT. Если вопрос не про статистику видео — верни: SELECT 0

Схема БД:

videos:
- id
- creator_id
- video_created_at
- views_count
- likes_count
- comments_count
- reports_count
- created_at
- updated_at

video_snapshots:
- id
- video_id (ссылка на videos.id)
- views_count, likes_count, comments_count, reports_count
- delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count
- created_at
- updated_at

Как выбирать таблицу и поля:

Итоговые значения (всего/общее количество X у всех видео) -> videos:
- просмотры    -> SUM(views_count)
- лайки        -> SUM(likes_count)
- комментарии  -> SUM(comments_count)
- жалобы       -> SUM(reports_count)

Прирост/прибавилось/добавилось/динамика X -> video_snapshots:
- просмотры    -> SUM(delta_views_count)
- лайки        -> SUM(delta_likes_count)
- комментарии  -> SUM(delta_comments_count)
- жалобы       -> SUM(delta_reports_count)

Фильтрация по датам:
- для снапшотов по дате: DATE(created_at) = 'YYYY-MM-DD'
- для даты публикации видео: DATE(video_created_at) ...
- video_created_at существует только в videos

Важно:
- всегда используй COALESCE(SUM(...), 0), чтобы результат был числом даже если строк нет
- если вопрос про комментарии — используй comments_count / delta_comments_count (не views_count)
- если вопрос про жалобы — используй reports_count / delta_reports_count (не likes_count)

Примеры:

1) Общее количество лайков у всех видео:
SELECT COALESCE(SUM(likes_count), 0) FROM videos;

2) Прирост просмотров за дату 2025-11-28:
SELECT COALESCE(SUM(delta_views_count), 0)
FROM video_snapshots
WHERE DATE(created_at) = '2025-11-28';

3) Сколько видео опубликовано за май 2025:
SELECT COUNT(*)
FROM videos
WHERE video_created_at >= '2025-05-01'::date AND video_created_at < '2025-06-01'::date;

Если месяц указан словами (например: "май 2025", "июль 2025") — преобразуй в диапазон
[первый день месяца; первый день следующего месяца).

Текущая дата: {current_date}
""".strip()


class GigaChatClient:
    def __init__(self) -> None:
        self._token: str | None = None
        self._exp: float = 0.0

    async def _token_get(self) -> str:
        if self._token and time.time() < self._exp - 30:
            return self._token

        headers = {
            "Authorization": f"Basic {AUTH_KEY}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "RqUID": str(uuid.uuid4()),
        }

        async with aiohttp.ClientSession() as s:
            async with s.post(OAUTH_URL, headers=headers, data={"scope": SCOPE}, ssl=False) as r:
                j = await r.json()

        self._token = j.get("access_token")
        if not self._token:
            raise RuntimeError("token")

        if "expires_at" in j:
            self._exp = float(j["expires_at"]) / 1000.0
        else:
            self._exp = time.time() + int(j.get("expires_in", 1500))

        return self._token

    async def sql(self, text: str) -> str:
        token = await self._token_get()

        payload = {
            "model": "GigaChat",
            "messages": [
                {"role": "system", "content": build_system_prompt()},
                {"role": "user", "content": text},
            ],
            "temperature": 0.0,
            "max_tokens": 240,
        }

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        async with aiohttp.ClientSession() as s:
            async with s.post(CHAT_URL, headers=headers, json=payload, ssl=False) as r:
                j = await r.json()

        return j["choices"][0]["message"]["content"].strip()