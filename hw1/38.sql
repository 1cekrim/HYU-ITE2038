SELECT
    Pokemon.name
FROM
    Evolution,
    Pokemon
WHERE
    Evolution.after_id NOT IN (
        SELECT
            Evolution.before_id
        FROM
            Evolution
    )
    AND Evolution.after_id = Pokemon.id
ORDER BY
    Pokemon.name;