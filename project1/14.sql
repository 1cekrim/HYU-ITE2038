-- 14
SELECT
    Pokemon.name
FROM
    Pokemon
WHERE
    Pokemon.type = 'Grass'
    AND Pokemon.id IN (
        SELECT
            Evolution.before_id
        FROM
            Evolution
    );