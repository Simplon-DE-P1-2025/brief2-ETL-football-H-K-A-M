-- # Indicateurs globaux
--   ##Nombre total de matchs disputés en Coupe du Monde
SELECT COUNT(*) AS total_matches_world_cup
FROM matches_normalized
WHERE is_final = true;


  -- ##Nombre total de matchs par édition si seulement de la coupe ( true) sinon false
SELECT edition, COUNT(*) AS total_matches
FROM matches_normalized
WHERE is_final = true
GROUP BY edition
ORDER BY edition;

  -- ##Nombre total de buts marqués toutes éditions confondues
SELECT
  SUM(h."Number_of_goals_scored" + away."Number_of_goals_scored") AS total_goals
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats away ON m.id_match = away.id_match
WHERE m.is_final = true;


-- ##Moyenne globale des buts marqués sur toutes éditions
SELECT
  ROUND(AVG(h."Number_of_goals_scored" + away."Number_of_goals_scored"), 2) AS avg_goals_per_match
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats away ON m.id_match = away.id_match
WHERE m.is_final = true;




-- ##Répartition des matchs par édition ( a voir si en prend les preliminary)
SELECT edition,
       COUNT(id_match) AS nb_matches
FROM matches_normalized
-- WHERE is_final = true
GROUP BY edition
ORDER BY edition;



-- ##Résultats des matchs
-- ##Nombre de victoires de l’équipe à domicile
SELECT COUNT(*) AS home_wins
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats away ON m.id_match = away.id_match
WHERE h."Number_of_goals_scored" > away."Number_of_goals_scored"
  AND m.is_final = true;




-- ##Nombre de victoires de l’équipe à l’extérieur
-- ##comme celle d’avant


-- ##Nombre de matchs nuls
SELECT COUNT(id_match) AS draws
FROM matches_normalized
WHERE result != 0
  AND is_final = true;


-- ##Nombre de matchs joués par équipe
SELECT t."Team_name",
       COUNT(id_match) AS matches_played
FROM (
  SELECT id_match, id_team FROM home_stats
  UNION ALL
  SELECT id_match, id_team FROM away_stats
) mt
JOIN teams_reference t ON mt.id_team = t.id_team
GROUP BY t."Team_name"
ORDER BY matches_played DESC;


-- ##Nombre total de victoires par équipe
SELECT t."Team_name",
       COUNT(id_match) AS wins
FROM matches_normalized m
JOIN home_team h ON m.id_match = h.id_match
JOIN away_team a ON m.id_match = a.id_match
JOIN dim_teams t
  ON (h.id_team = t.id_team AND h.goal_by_team > a.goal_by_team)
  OR (a.id_team = t.id_team AND a.goal_by_team > h.goal_by_team)
WHERE m.is_final = true
GROUP BY t."Team_name"
ORDER BY wins DESC;




##Nombre total de défaites par équipe
##la mm qu’avant a inversé


##Nombre total de matchs nuls par équipe
SELECT t.name_team,
       COUNT(*) AS draw
FROM matches_final_kpi m
JOIN home_team h ON m.id_match = h.id_match
JOIN away_team a ON m.id_match = a.id_match
JOIN dim_teams t
  ON (h.id_team = t.id_team
  OR (a.id_team = t.id_team
WHERE m.is_final = true and m.result = ‘draw’
GROUP BY t.name_team
ORDER BY wins DESC;


###Nombre de buts marqués par équipe
SELECT t.name_team,
       SUM(goal_by_team) AS goals_scored
FROM (
  SELECT id_team, goal_by_team FROM home_team
  UNION ALL
  SELECT id_team, goal_by_team FROM away_team
) g
JOIN dim_teams t ON g.id_team = t.id_team
GROUP BY t.name_team
ORDER BY goals_scored DESC;




--##Nombre de buts encaissés par équipe
SELECT t."Team_name",
       SUM(g."Number_of_goals_conceded") AS goals_conceded
FROM (
    SELECT id_team, "Number_of_goals_conceded" FROM home_stats
    UNION ALL
    SELECT id_team, "Number_of_goals_conceded" FROM away_stats
) g
JOIN teams_reference t ON g.id_team = t.id_team
GROUP BY t."Team_name"
ORDER BY goals_conceded DESC;


--##Analyses par édition

--Nombre de matchs par édition
SELECT edition,
       COUNT(id_match) AS nb_matches
FROM matches_normalized
WHERE is_final = true
GROUP BY edition
ORDER BY edition;


--Nombre total de buts par édition
SELECT m.edition,
       SUM(h."Number_of_goals_scored" + a."Number_of_goals_scored") AS total_goals
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
WHERE m.is_final = true
GROUP BY m.edition
ORDER BY m.edition;


##Moyenne de buts par match par édition
SELECT m.edition,
       ROUND(
         AVG(h."Number_of_goals_scored" + a."Number_of_goals_scored"), 2
       ) AS avg_goals_per_match
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
WHERE m.is_final = true
GROUP BY m.edition
ORDER BY m.edition;


--Édition la plus offensive (plus de buts par match)


--Édition avec le plus grand nombre de matchs



--##Analyses par tour (round)

-- #Moyenne de buts par tour
SELECT m.round,
       ROUND(
         AVG(h."Number_of_goals_scored" + a."Number_of_goals_scored"), 2
       ) AS avg_goals
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
WHERE m.is_final = true
GROUP BY m.round
ORDER BY avg_goals DESC;


-- #Répartition des résultats par tour
SELECT round,
       SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END) AS draws,
       SUM(CASE WHEN result > 0 THEN 1 ELSE 0 END) AS wins
FROM matches_normalized
WHERE is_final = true
GROUP BY round
ORDER BY round;




-- #Taux de matchs nuls par tour
SELECT round,
       ROUND(
         SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END)::numeric
         / COUNT(*) * 100, 2
       ) AS draw_rate_pct
FROM matches_normalized
WHERE is_final = true
GROUP BY round
ORDER BY draw_rate_pct DESC;

-- #Analyses géographiques
-- Nombre de matchs joués par ville
SELECT city,
       COUNT(id_match) AS matches
FROM matches_normalized
WHERE is_final = true
GROUP BY city
ORDER BY matches DESC;



-- #Villes les plus utilisées lors d’une édition
SELECT edition, city, COUNT(id_match) AS matches
FROM matches_normalized
WHERE is_final = true
GROUP BY edition, city
ORDER BY edition, matches DESC;


-- #Analyses temporelles
-- #Évolution de la moyenne de buts par match au fil des éditions
SELECT m.edition,
       ROUND(
         AVG(h."Number_of_goals_scored" + a."Number_of_goals_scored"), 2
       ) AS avg_goals
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
WHERE m.is_final = true
GROUP BY m.edition
ORDER BY m.edition;




-- #Évolution du taux de matchs nuls dans le temps
SELECT edition,
       ROUND(
         SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END)::numeric
         / COUNT(*) * 100, 2
       ) AS draw_rate_pct
FROM matches_normalized
WHERE is_final = true
GROUP BY edition
ORDER BY edition;




-- #Confrontations & historique
-- #Nombre de confrontations entre deux équipes
SELECT t1."Team_name" AS home_team,
       t2."Team_name" AS away_team,
       COUNT(*) AS matches
FROM home_stats h
JOIN away_stats a ON h.id_match = a.id_match
JOIN teams_reference t1 ON h.id_team = t1.id_team
JOIN teams_reference t2 ON a.id_team = t2.id_team
GROUP BY home_team, away_team
ORDER BY matches DESC;





-- #Indicateurs avancés (bonus)
-- #Taux de victoire par équipe
SELECT t."Team_name",
       ROUND(
         SUM(CASE
               WHEN (h.id_team = t.id_team AND h."Number_of_goals_scored" > a."Number_of_goals_scored")
                 OR (a.id_team = t.id_team AND a."Number_of_goals_scored" > h."Number_of_goals_scored")
               THEN 1 ELSE 0 END)::numeric
         / COUNT(*) * 100, 2
       ) AS win_rate_pct
FROM matches_normalized m
JOIN home_stats h ON m.id_match = h.id_match
JOIN away_stats a ON m.id_match = a.id_match
JOIN teams_reference t ON t.id_team IN (h.id_team, a.id_team)
WHERE m.is_final = true
GROUP BY t."Team_name"
ORDER BY win_rate_pct DESC;


-- #Performance moyenne d’une équipe par édition
SELECT m.edition,
       t."Team_name",
       ROUND(AVG(g."Number_of_goals_scored"), 2) AS avg_goals
FROM matches_normalized m
JOIN (
  SELECT id_match, id_team, "Number_of_goals_scored" FROM home_stats
  UNION ALL
  SELECT id_match, id_team, "Number_of_goals_scored" FROM away_stats
) g ON m.id_match = g.id_match
JOIN teams_reference t ON g.id_team = t.id_team
WHERE m.is_final = true
GROUP BY m.edition, t."Team_name"
ORDER BY m.edition, avg_goals DESC;


-- #Performance des équipes selon le tour
SELECT m.round,
       t."Team_name",
       ROUND(AVG(g."Number_of_goals_scored"), 2) AS avg_goals
FROM matches_normalized m
JOIN (
  SELECT id_match, id_team, "Number_of_goals_scored" FROM home_stats
  UNION ALL
  SELECT id_match, id_team, "Number_of_goals_scored" FROM away_stats
) g ON m.id_match = g.id_match
JOIN teams_reference t ON g.id_team = t.id_team
WHERE m.is_final = true
GROUP BY m.round, t."Team_name"
ORDER BY m.round, avg_goals DESC;


-- #Probabilité historique de victoire d’une équipe face à une autre
SELECT t1."Team_name" AS team_1,
       t2."Team_name" AS team_2,
       ROUND(
         SUM(CASE WHEN h."Number_of_goals_scored" > a."Number_of_goals_scored" THEN 1 ELSE 0 END)::numeric
         / COUNT(*) * 100, 2
       ) AS win_probability_team_1
FROM home_stats h
JOIN away_stats a ON h.id_match = a.id_match
JOIN teams_reference t1 ON h.id_team = t1.id_team
JOIN teams_reference t2 ON a.id_team = t2.id_team
GROUP BY team_1, team_2
ORDER BY win_probability_team_1 DESC;
