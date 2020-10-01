SELECT
    first.id,
    first.name AS first,
    second.name AS second,
    third.name AS third
FROM
    Pokemon AS first
    JOIN Pokemon AS second ON second.id = (
        SELECT
            after_id
        FROM
            Evolution
        WHERE
            Evolution.before_id = first.id
    )
    JOIN Pokemon AS third ON third.id = (
        SELECT
            after_id
        FROM
            Evolution
        WHERE
            Evolution.before_id = second.id
    )
ORDER BY
    first.id;