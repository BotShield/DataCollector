/* Viewdefinitionen für BotShield zur Implementierung der Kennzahlen 
Autor: jstrebel
*/

DROP VIEW IF EXISTS V_USER_ALLKPI,V_USER_KPI_ALLPOSTS,V_USER_KPI_PROFILEAGE,V_USER_KPI_NAMDESCLEN,V_USER_KPI_ANZFOLLOW CASCADE;

-- Indikator "Alter des Profils"
CREATE VIEW V_USER_KPI_PROFILEAGE as 
select ID USERID, extract(epoch from age(MCA))/(3600*24) PROFILEAGE from
(select ID, min(created_at) MCA from t_user group by ID) as mindates;

-- Indikator "Anzahl aller vorhandenen Posts"
CREATE VIEW V_USER_KPI_ALLPOSTS as 
select status_user_id USERID, count(*) ALLPOSTS from T_STATUS group by status_user_id;

-- Indikator "Länge des Profilnamens und Länge des Beschreibungstextes"
CREATE VIEW V_USER_KPI_NAMDESCLEN as 
select ID USERID, COALESCE(avg(char_length(screen_name)),0) PROFILENAMELEN, 
COALESCE(avg(char_length(description)),0) DESCLEN from T_USER group by ID;

-- Indikator "Anzahl Follower"
-- TODO: Zeitliche Entwicklung der Follower, Friends
CREATE VIEW V_USER_KPI_ANZFOLLOW as 
select ID USERID, avg(followers_count) ANZFOLLOWER, avg(friends_count) ANZFRIENDS, 
avg(followers_count)/NULLIF(avg(friends_count),0) RATIOFOLFRIEND from T_USER group by ID;


CREATE VIEW V_USER_ALLKPI as 
select t1.USERID,t1.PROFILEAGE,
t2.ALLPOSTS, 
t3.PROFILENAMELEN, t3.DESCLEN, 
t4.ANZFOLLOWER, t4.ANZFRIENDS, t4.RATIOFOLFRIEND
from V_USER_KPI_PROFILEAGE t1, V_USER_KPI_ALLPOSTS t2, V_USER_KPI_NAMDESCLEN t3, V_USER_KPI_ANZFOLLOW t4
where t1.USERID=t2.USERID and t1.USERID=t3.USERID and t1.USERID=t4.USERID;
