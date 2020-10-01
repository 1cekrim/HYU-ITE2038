--2
SELECT
    Pokemon.name
FROM
    Pokemon
WHERE
    Pokemon.type IN (
        SELECT
            SUBQ.type
        FROM
            (
                SELECT
                    Pokemon.type,
                    COUNT(*) AS typeCount
                FROM
                    Pokemon
                GROUP BY
                    Pokemon.type
                ORDER BY
                    typeCount desc
            ) AS SUBQ,
            (
                SELECT
                    Pokemon.type,
                    COUNT(*) AS typeCount
                FROM
                    Pokemon
                GROUP BY
                    Pokemon.type
                ORDER BY
                    typeCount desc
                LIMIT
                    2
            ) AS TOP
        WHERE
            SUBQ.typeCount IN (
                SELECT
                    TOP.typeCount
            )
    )
ORDER BY
    Pokemon.name;