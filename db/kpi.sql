# Indicateurs globaux
  ##Nombre total de matchs disputés en Coupe du Monde
SELECT COUNT(*) AS total_matches_world_cup
FROM matches_final_kpi
WHERE is_final = true;


  ##Nombre total de matchs par édition si seulement de la coupe ( true) sinon false
SELECT edition, COUNT(*) AS total_matches
FROM matches_final_kpi
WHERE is_final = true
GROUP BY edition
ORDER BY edition;


  ##Nombre total de buts marqués toutes éditions confondues
SELECT
  SUM(h.goal_by_team + a.goal_by_team) AS total_goals
FROM matches_final_kpi m
JOIN home_team h ON m.id_match = h.id_match
JOIN away_team a ON m.id_match = a.id_match
WHERE m.is_final = true;




  ##Moyenne de buts par match
SELECT
  AVG(h.goal_by_team + a.goal_by_team) AS avg_goals_per_match
FROM matches_final_kpi m
JOIN home_team h ON m.id_match = h.id_match
JOIN away_team a ON m.id_match = a.id_match
WHERE m.is_final = true;




  ##Répartition des matchs par édition ( a voir si en prend les preliminary)
SELECT edition,
       COUNT(*) AS nb_matches
FROM matches_final_kpi
GROUP BY edition
ORDER BY edition;

  ##Résultats des matchs
  ##Nombre de victoires de l’équipe à domicile
SELECT COUNT(*) AS home_wins
FROM matches_final_kpi m
JOIN home_team h ON m.id_match = h.id_match
JOIN away_team a ON m.id_match = a.id_match
WHERE h.goal_by_team > a.goal_by_team
  AND m.is_final = true;




##Nombre de victoires de l’équipe à l’extérieur
##comme celle d’avant


##Nombre de matchs nuls
SELECT COUNT(*) AS draws
FROM matches_final_kpi
WHERE result = 'draw'
  AND is_final = true;


##Proportion des victoires domicile vs extérieur


##Taux de matchs nuls par édition
##Analyses par équipe
##Nombre de matchs joués par équipe
SELECT t.name_team,
       COUNT(*) AS matches_played
FROM (
  SELECT id_team FROM home_team
  UNION ALL
  SELECT id_team FROM away_team
) mt
JOIN dim_teams t ON mt.id_team = t.id_team
GROUP BY t.name_team
ORDER BY matches_played DESC;




##Nombre total de victoires par équipe
SELECT t.name_team,
       COUNT(*) AS wins
FROM matches_final_kpi m
JOIN home_team h ON m.id_match = h.id_match
JOIN away_team a ON m.id_match = a.id_match
JOIN dim_teams t
  ON (h.id_team = t.id_team AND h.goal_by_team > a.goal_by_team)
  OR (a.id_team = t.id_team AND a.goal_by_team > h.goal_by_team)
WHERE m.is_final = true
GROUP BY t.name_team
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




##Nombre de buts encaissés par équipe


##Analyses par édition
##Nombre de matchs par édition


##Nombre total de buts par édition


##Moyenne de buts par match par édition


##Édition la plus offensive (plus de buts par match)


##Édition avec le plus grand nombre de matchs
SELECT edition, COUNT(*) AS matches
FROM matches_final_kpi
GROUP BY edition
ORDER BY matches DESC
LIMIT 1;


##Analyses par tour (round)
##Nombre de matchs par tour
SELECT round, COUNT(*) AS matches
FROM matches_final_kpi
GROUP BY round
ORDER BY matches DESC;



#Moyenne de buts par tour


#Répartition des résultats par tour


#Comparaison du nombre de buts entre phase de groupes et phases finales


#Taux de matchs nuls par tour
#Analyses géographiques
#Nombre de matchs joués par ville
SELECT city, COUNT(*) AS matches
FROM matches_final_kpi
GROUP BY city
ORDER BY matches DESC;




Top villes ayant accueilli le plus de matchs


SELECT city, COUNT(*) AS matches
FROM matches_final_kpi
GROUP BY city
ORDER BY matches DESC
LIMIT 10;

#Répartition des matchs par ville


#Villes les plus utilisées lors d’une édition
SELECT edition, city, COUNT(*) AS matches
FROM matches_final_kpi
GROUP BY edition, city
ORDER BY edition, matches DESC;


#Analyses temporelles
#Évolution du nombre de buts par match au fil des éditions
SELECT edition, COUNT(*) AS matches
FROM matches_final_kpi
GROUP BY edition
ORDER BY edition;




#Évolution du taux de matchs nuls dans le temps


#Comparaison des performances entre éditions successives


#Confrontations & historique
#Nombre de confrontations entre deux équipes
SELECT
  t1.name_team AS team_1,
  t2.name_team AS team_2,
  COUNT(*) AS matches
FROM home_team h
JOIN away_team a ON h.id_match = a.id_match
JOIN dim_teams t1 ON h.id_team = t1.id_team
JOIN dim_teams t2 ON a.id_team = t2.id_team
GROUP BY team_1, team_2
ORDER BY matches DESC;





#Indicateurs avancés (bonus)
#Taux de victoire par équipe


#Performance moyenne d’une équipe par édition


#Performance des équipes selon le tour


#Probabilité historique de victoire d’une équipe face à une autre
