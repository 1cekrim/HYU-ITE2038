SELECT
    Pokemon.type
FROM
    Pokemon,
    Evolution
WHERE
    Evolution.before_id = Pokemon.id
GROUP BY
    Pokemon.type
HAVING
    COUNT(*) >= 3
ORDER BY
    Pokemon.type desc;