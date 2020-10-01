-- 16
SELECT
    COUNT(*)
FROM
    Pokemon
WHERE
    Pokemon.type = 'Water'
    OR Pokemon.type = 'Electric'
    OR Pokemon.type = 'Psychic';