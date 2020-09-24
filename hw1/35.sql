SELECT
    Pokemon.name
FROM
    Evolution,
    Pokemon
WHERE
    Evolution.before_id > Evolution.after_id
    AND Pokemon.id = Evolution.before_id
ORDER BY
    Pokemon.name;