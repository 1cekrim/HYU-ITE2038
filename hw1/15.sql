-- 15
SELECT
    SUBQ.owner_id,
    SUBQ.catchCount
FROM
    (
        SELECT
            CatchedPokemon.owner_id,
            COUNT(*) AS catchCount
        FROM
            CatchedPokemon
        GROUP BY
            CatchedPokemon.owner_id
        ORDER BY
            catchCount desc
    ) AS SUBQ,
    (
        SELECT
            CatchedPokemon.owner_id,
            COUNT(*) AS catchCount
        FROM
            CatchedPokemon
        GROUP BY
            CatchedPokemon.owner_id
        ORDER BY
            catchCount desc
        LIMIT
            1
    ) AS TOP
WHERE
    SUBQ.catchCount IN (
        SELECT
            TOP.catchCount
    )
ORDER BY
    SUBQ.owner_id;