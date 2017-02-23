-- Tabellendefinitionen für die Speicherung der Daten aus der Twitter4J-API
-- Passend für eine PostgreSQL 9.6 Datenbank
-- Status: erledigt (Einschränkugen siehe unten)

DROP SEQUENCE IF EXISTS url_seq,param_seq,place_seq,geoloc_seq,entity_seq,hashtag_seq CASCADE;
CREATE SEQUENCE url_seq; -- eine Sequence, um die URLs eines Tweets mit einer Nummer auszustatten.
CREATE SEQUENCE param_seq; -- eine Sequence für die IDs einer Datensammel-Sitzung.
CREATE SEQUENCE place_seq; -- eine Sequence für die IDs eines Place.
CREATE SEQUENCE geoloc_seq; -- eine Sequence für die IDs einer Geolocation.
CREATE SEQUENCE entity_seq; -- eine Sequence für die IDs einer Entity.
CREATE SEQUENCE hashtag_seq; -- eine Sequence für die IDs einer Entity.

DROP TABLE IF EXISTS T_Attribut, T_Hashtag, T_Symbol, T_URL, T_User_Mention, T_MediaEntitySize, T_Media, T_Entity, T_Place, T_Status , T_User,T_DataCollParameter, T_Geolocation CASCADE;

CREATE TABLE T_DataCollParameter --Data Collector Parameters
(
 ID bigint PRIMARY KEY,
 track_topics  VARCHAR (4000),
 datasource VARCHAR (4000) -- streaming or rest 
);


CREATE  TABLE T_Attribut
(
 ID bigint PRIMARY KEY,
 KEY   VARCHAR (4000) ,
 Value VARCHAR (4000) 
);

-- Ein Place kann Teil eines anderen Place sein
-- RateLimitStatus habe ich als Meta-Datum der TwitterResponse ausgeschlossen
CREATE  TABLE  T_Place
(
    ID   bigint PRIMARY KEY,
    pname VARCHAR (100) ,
    pfullname VARCHAR (4000) ,
    place_url  VARCHAR (4000) ,
    bb_type VARCHAR (4000),
    geo_type VARCHAR (4000),
    country VARCHAR (4000),
    country_code VARCHAR (4000),  
    place_type VARCHAR (4000),
    street_address VARCHAR (4000),    
    contained_place_id  bigint REFERENCES T_Place(ID)
);

-- Annahme: es gibt nur ein Polygon pro BoundingBox und Geometry in Place
CREATE  TABLE T_Geolocation
(
 ID bigint PRIMARY KEY,
 latitude double precision,
 longitude double precision,
 bboxcoord_place_id  bigint REFERENCES T_Place(ID),
 geocoord_place_id  bigint REFERENCES T_Place(ID)
);


/* Noch ohne ExtendedMedia
* Tabelle für die Klasse Entity, deren abgeleitete Klassen dann 
* ExtendedMediaEntity, HashtagEntity, MediaEntity, SymbolEntity, URLEntity, UserMentionEntity sind
*/
CREATE  TABLE T_Entity
(      
       ID bigint PRIMARY KEY
);


CREATE  TABLE T_Hashtag
(
 ID bigint PRIMARY KEY,
    indices_start integer ,
    indices_end   integer,
    httext        VARCHAR (4000),
    entity_id	  bigint REFERENCES T_Entity(ID)
);

CREATE  TABLE T_Symbol
(
 ID bigint PRIMARY KEY,
    indices_start integer ,
    indices_end   integer,
    symtext        VARCHAR (4000),
    entity_id	  bigint REFERENCES T_Entity(ID)
);

/* T_Status
Feld contributors fehlt, weil deprecated laut API-Doku
Keine Ref. Integrität für quoted und retweeted tweets, da sonst eine rekursive Auflösung dieser Tweets erfolgen muss.
Feld scopes fehlt, weil nur für Twitter-Werbung
Feld WithheldInCountries enthält die String-Verkettung aus der Twitter4J API.
T_Geolocation wird entfernt und deren zwei Felder werden dem Status zugeschlagen
TODO: Entitys einbauen
*/
CREATE  TABLE T_Status
(
    ID   bigint,
    recorded_at  TIMESTAMP WITH TIME ZONE,
    created_at  TIMESTAMP WITH TIME ZONE ,
    favourites_count integer ,
    username        VARCHAR (4000) ,
    screen_name VARCHAR (4000) ,
    lang            VARCHAR (4000) ,
    status_place_id  bigint REFERENCES T_Place(ID), 
    withheld_in_countries VARCHAR (4000),
    InReplyToScreenName varchar(4000),
    InReplyToStatusId bigint,
    InReplyToUserId bigint,
    quoted_status_id  bigint,
    RetweetCount integer,
    retweeted_status_id bigint,
    status_source varchar(4000),
    status_Text varchar(4000),
    status_user_id bigint,
    isFavorited integer,
    isPossiblySensitive integer,
    isRetweet integer,
    isRetweeted integer,
    isRetweetedByMe integer,
    isTruncated integer,
    dcparam_id bigint REFERENCES T_DataCollParameter(ID),
    latitude double precision,
    longitude double precision,
    URLEntities_id bigint REFERENCES T_Entity(ID),
    HashtagEntities_id bigint REFERENCES T_Entity(ID),
    PRIMARY KEY (ID,recorded_at)
);

/*
T_User:
Folgende Felder sind nicht implementiert:
String	getOriginalProfileImageURL() 
String	getOriginalProfileImageURLHttps() 
String	getProfileBackgroundColor() 
String	getProfileBackgroundImageURL() 
String	getProfileBackgroundImageUrlHttps() 
String	getProfileBannerIPadRetinaURL() 
String	getProfileBannerIPadURL() 
String	getProfileBannerMobileRetinaURL() 
String	getProfileBannerMobileURL() 
String	getProfileBannerRetinaURL() 
String	getProfileBannerURL()
String	getProfileImageURL()
String	getProfileLinkColor() 
String	getProfileSidebarBorderColor() 
String	getProfileSidebarFillColor() 
String	getProfileTextColor() 
boolean	isShowAllInlineMedia()
Da sich das User-Profil im Zeitverlauf ändern kann, brauchen wir einen Zeitstempel im Primary Key. Es macht keinen Sinn, die User-Daten in der DB zu aktualisieren.
User und Status sind 1:1 verknüpft, nicht 1:N, wie ich früher dachte.
*/
CREATE  TABLE T_User
(
    ID   bigint, --user ID, not status ID!!
    recorded_at  TIMESTAMP WITH TIME ZONE,
    username        VARCHAR (4000) ,
    screen_name VARCHAR (4000) ,
    created_at  TIMESTAMP WITH TIME ZONE ,
    description     VARCHAR (4000) ,
    geo_enabled     integer,
    lang            VARCHAR (4000) ,
    followers_count integer ,
    favourites_count integer ,
    friends_count   integer ,
    listed_count   integer ,
    loca            VARCHAR (4000),
    statuses_count        integer ,
    TimeZone varchar(4000),
    user_URL varchar(4000),
    URLEntity_id bigint,
    DescURLEntity_id bigint,
    UtcOffset integer, 
    WithheldInCountries varchar(4000),
    isContributorsEnabled integer,
    isDefaultProfile integer,
    isDefaultProfileImage integer,
    isFollowRequestSent integer,
	isProfileBackgroundTiled integer,
	isProfileUseBackgroundImage integer,
	isProtected integer,
	isTranslator integer,
	isverified  integer,
	PRIMARY KEY (ID,recorded_at)
);
ALTER TABLE T_Status ADD CONSTRAINT fk_uid FOREIGN KEY (status_user_id,recorded_at) REFERENCES T_User(ID,recorded_at);

CREATE  TABLE T_URL
(
    ID bigint,
    display_url   VARCHAR (4000) ,
    expanded_url  VARCHAR (4000) ,
    indices_start integer ,
    indices_end   integer ,
    url           VARCHAR (4000),
    urltext	  VARCHAR (4000),
    entity_id	  bigint REFERENCES T_Entity(ID),
    PRIMARY KEY (ID)
);

ALTER TABLE T_User ADD FOREIGN KEY (URLEntity_id) REFERENCES T_Entity(ID);
ALTER TABLE T_User ADD FOREIGN KEY (DescURLEntity_id) REFERENCES T_Entity(ID);

CREATE  TABLE T_User_Mention
(
    ID bigint PRIMARY KEY,
    user_id       bigint,
    indices_start integer ,
    indices_end   integer ,
    username      VARCHAR (4000),
    screen_name   VARCHAR (4000),
    umtext	  VARCHAR (4000),
    entity_id	  bigint REFERENCES T_Entity(ID)  
);


-- Ich bilde die Vererbung in Twitter4J über eine einfache Wiederholung der Felder aus der Basisklasse URL ab.
CREATE  TABLE T_Media
(
    ID               bigint PRIMARY KEY,
    media_url        VARCHAR (4000) ,
    media_url_https  VARCHAR (4000) ,
    media_type             VARCHAR (4000) , 
    display_url   VARCHAR (4000) ,
    expanded_url  VARCHAR (4000) ,
    indices_start integer ,
    indices_end   integer ,
    url           VARCHAR (4000),
    urltext	  VARCHAR (4000),
    entity_id	  bigint REFERENCES T_Entity(ID)
);

CREATE  TABLE T_MediaEntitySize
(
    ID bigint,
    size          integer, -- large, medium, small, thumb als Twitter4j-Konstante 
    height	  integer,
    resize	  integer,
    width	  integer,
    media_id	  bigint REFERENCES T_Media(ID),
    PRIMARY KEY (ID, size)
);

/* TODO - sehr komplexer Typ mit vielen vererbten Feldern in Twitter4J. Das würde ich mir für den ersten Wurf erstmal sparen.
CREATE  TABLE ExtendedMediaEntityVariant
(
    ID bigint PRIMARY KEY,
    bitrate       integer,
    contenttype	  varchar(4000),
    media_url	  varchar(4000),
    variant_id	  bigint REFERENCES ExtendedMedia(ID),
);*/

/* TODO - sehr komplexer Typ mit vielen vererbten Feldern in Twitter4J. Das würde ich mir für den ersten Wurf erstmal sparen.
CREATE  TABLE ExtendedMedia
(
    ID               bigint,
    

);*/

