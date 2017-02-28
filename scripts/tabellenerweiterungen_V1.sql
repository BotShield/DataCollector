/*
Erweiterungen des existierenden Datenmodells aus "tabellendefinitionen.sql"
Dient zum Ergänzung  einer bestehenden Twitter-Datenbank
*/

CREATE SEQUENCE usermention_seq; -- eine Sequence für die IDs einer UserMention.
ALTER TABLE T_Status ADD COLUMN IF NOT EXISTS UserMentionEntities_id bigint;
ALTER TABLE T_Status ADD FOREIGN KEY (UserMentionEntities_id) REFERENCES T_Entity(ID); 
