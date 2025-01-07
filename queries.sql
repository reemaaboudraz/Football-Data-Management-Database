-- 1. Basic select with simple WHERE clause
SELECT * 
FROM team
WHERE league_id = 1;

-- 2. Basic select with simple GROUP BY clause (with and without HAVING clause)
-- Without HAVING
SELECT league_id, COUNT(*)
FROM team
GROUP BY league_id;

-- With HAVING
SELECT league_id, COUNT(*)
FROM team
GROUP BY league_id
HAVING COUNT(*) > 5;

-- 3. Simple join query and equivalent Cartesian product with WHERE clause
-- INNER JOIN
SELECT p.first_name, p.last_name, t.name AS team_name
FROM player p
JOIN team_member tm ON p.id = tm.player_id
JOIN team t ON tm.team_id = t.id;

-- Equivalent Cartesian product with WHERE clause
SELECT p.first_name, p.last_name, t.name AS team_name
FROM player p, team t, team_member tm
WHERE p.id = tm.player_id
AND tm.team_id = t.id;

-- 4. Demonstrating various join types

-- INNER JOIN
SELECT p.first_name, p.last_name, t.name AS team_name
FROM player p
INNER JOIN team_member tm ON p.id = tm.player_id
INNER JOIN team t ON tm.team_id = t.id;

-- LEFT JOIN
SELECT p.first_name, p.last_name, t.name AS team_name
FROM player p
LEFT JOIN team_member tm ON p.id = tm.player_id
LEFT JOIN team t ON tm.team_id = t.id;

-- RIGHT JOIN
SELECT p.first_name, p.last_name, t.name AS team_name
FROM player p
RIGHT JOIN team_member tm ON p.id = tm.player_id
RIGHT JOIN team t ON tm.team_id = t.id;

-- FULL JOIN
SELECT p.first_name, p.last_name, t.name AS team_name
FROM player p
FULL JOIN team_member tm ON p.id = tm.player_id
FULL JOIN team t ON tm.team_id = t.id;

-- 5. Queries using NULL values

-- Find players not assigned to any team (team_id is NULL)
SELECT p.first_name, p.last_name
FROM player p
LEFT JOIN team_member tm ON p.id = tm.player_id
WHERE tm.team_id IS NULL;

-- 6. Correlated query example

-- Find players who have played more games than the average number of games played by all players in the same league
SELECT p.first_name, p.last_name, ps.games_played
FROM player p
JOIN player_stats ps ON p.id = ps.player_id
WHERE ps.games_played > (
    SELECT AVG(games_played)
    FROM player_stats
    WHERE league_id = ps.league_id
);

-- 7. Set operations

-- INTERSECT: Find players who played in both League 1 and League 2
SELECT player_id
FROM player_stats
WHERE league_id = 1
INTERSECT
SELECT player_id
FROM player_stats
WHERE league_id = 2;

-- UNION: Combine all unique player_ids who have played in League 1 or League 2
SELECT player_id
FROM player_stats
WHERE league_id = 1
UNION
SELECT player_id
FROM player_stats
WHERE league_id = 2;

-- EXCEPT (Difference): Find players who have played in League 1 but not in League 2
SELECT player_id
FROM player_stats
WHERE league_id = 1
EXCEPT
SELECT player_id
FROM player_stats
WHERE league_id = 2;

-- 8. View with hard-coded criteria

-- Hardcoded view to show player stats with a market value > 1,000,000
CREATE VIEW player_stats_with_hardcode AS
SELECT
    p.id,
    p.first_name,
    p.last_name,
    ps.team_id,
    ps.league_id,
    ps.games_played,
    ps.goals,
    ps.assists,
    ps.yellow_cards,
    ps.red_cards,
    ps.market_value
FROM player p
JOIN player_stats ps ON p.id = ps.player_id
WHERE ps.market_value > 1000000;  -- Hardcoded criteria

-- 9. Overlap and covering constraints

-- Overlap constraint: Players who play in multiple leagues (simulating overlap of teams and leagues)
SELECT DISTINCT p.first_name, p.last_name
FROM player p
JOIN player_stats ps ON p.id = ps.player_id
WHERE ps.league_id IN (1, 2);

-- Covering constraint: Ensure all players have played in both League 1 and League 2
SELECT p.first_name, p.last_name
FROM player p
JOIN player_stats ps ON p.id = ps.player_id
GROUP BY p.id
HAVING COUNT(DISTINCT ps.league_id) = 2
AND EXISTS (SELECT 1 FROM player_stats ps1 WHERE ps1.player_id = p.id AND ps1.league_id = 1)
AND EXISTS (SELECT 1 FROM player_stats ps2 WHERE ps2.player_id = p.id AND ps2.league_id = 2);



-- 10. Division operator

-- Using NOT IN (for division-like behavior)
SELECT t.name
FROM team t
WHERE NOT EXISTS (
    SELECT 1
    FROM player_stats ps
    WHERE ps.team_id = t.id
    AND ps.league_id = 1
)
AND NOT EXISTS (
    SELECT 1
    FROM player_stats ps
    WHERE ps.team_id = t.id
    AND ps.league_id = 2
);

-- Using NOT EXISTS (for division-like behavior)
SELECT t.name
FROM team t
WHERE NOT EXISTS (
    SELECT 1
    FROM player_stats ps
    WHERE ps.team_id = t.id
    AND ps.league_id = 1
    EXCEPT
    SELECT 1
    FROM player_stats ps
    WHERE ps.team_id = t.id
    AND ps.league_id = 2
);
