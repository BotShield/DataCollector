/* Viewdefinitionen für BotShield zur Implementierung der Kennzahlen 
Autor: jstrebel
*/

DROP VIEW IF EXISTS V_USER_KPI_PROFILEAGE,V_USER_KPI_ALLPOSTS CASCADE;

-- Indikator "Alter des Profils"
CREATE VIEW V_USER_KPI_PROFILEAGE as 
select ID USERID, extract(epoch from age(MCA))/(3600*24) PROFILEAGE from
(select ID, min(created_at) MCA from t_user group by ID) as mindates;

-- Indikator "Anzahl aller vorhandenen Posts"
CREATE VIEW V_USER_KPI_ALLPOSTS as 
select status_user_id USERID, count(*) ALLPOSTS from T_STATUS group by status_user_id;

-- Indikator "Länge des Profilnamens"
CREATE VIEW V_USER_KPI_PROFILENAMELEN as 
select ID USERID, avg(char_length(screen_name)) PROFILENAMELEN from T_USER group by ID;


CREATE VIEW V_USER_ALLKPI as 
select t1.USERID,t1.PROFILEAGE,t2.ALLPOSTS, t3.PROFILENAMELEN from V_USER_KPI_PROFILEAGE t1, V_USER_KPI_ALLPOSTS t2, V_USER_KPI_PROFILENAMELEN t3
where t1.USERID=t2.USERID and t1.USERID=t3.USERID;
