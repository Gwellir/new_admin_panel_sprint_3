"""Модуль содержит шаблоны запросов к БД Postgres."""

UPDATED_IDS_QUERY = """
    SELECT id, updated_at
    FROM "content".{table}
    WHERE updated_at > {modified}
    ORDER BY updated_at, id
    LIMIT {chunk_size};
    """
RELATED_FILM_WORK_QUERY = """
    SELECT fw.id, fw.updated_at
    FROM content.film_work fw
    LEFT JOIN content.{cross_table} cross_fw
        ON cross_fw.film_work_id = fw.id
    WHERE cross_fw.{cross_id} in ({ids})
    ORDER BY fw.updated_at, fw.id
    LIMIT {chunk_size};
    """
ENRICHED_DATA_QUERY = """
    SELECT
        fw.id as fw_id,
        fw.title as fw_title,
        fw.description as fw_description,
        fw.rating as fw_rating,
        fw.type as fw_type,
        pfw.role as p_role,
        p.id as p_id,
        p.full_name as p_full_name,
        g.name as g_genre
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw
        ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p
        ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw
        ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g on g.id = gfw.genre_id
    WHERE fw.id IN ({ids});
    """
