-- Tabellendefinitionen für die Speicherung der Daten aus der Twitter4J-API
-- Passend für eine PostgreSQL 9.6 Datenbank
-- Status: erledigt (Einschränkugen siehe unten)

DROP SEQUENCE IF EXISTS status_seq;
CREATE SEQUENCE status_seq; -- eine Sequence, um alle Teilobjekte eines Tweets mit einer Nummer auszustatten.

DROP TABLE IF EXISTS T_Attribut, T_Geolocation, T_Hashtag, T_Symbol, T_URL, T_User_Mention, T_MediaEntitySize, T_Media, T_Entity, T_Place, T_Status , T_User;

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


CREATE  TABLE T_Geolocation
(
 ID bigint PRIMARY KEY,
 latitude double precision,
 longitude double precision,
 bboxcoord_place_id  bigint REFERENCES T_Place(ID),
 geocoord_place_id  bigint REFERENCES T_Place(ID)
);

-- Noch ohne ExtendedMedia
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
*/
CREATE  TABLE T_Status
(
    ID   bigint,
    recorded_at  TIMESTAMP (0) WITH TIME ZONE,
    created_at  TIMESTAMP (0) WITH TIME ZONE ,
    favourites_count integer ,
    geoloc_id  bigint REFERENCES T_Geolocation(ID), 
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

*/
CREATE  TABLE T_User
(
    ID   bigint,
    recorded_at  TIMESTAMP (0) WITH TIME ZONE,
    username        VARCHAR (4000) ,
    screen_name VARCHAR (4000) ,
    created_at  TIMESTAMP (0) WITH TIME ZONE ,
    description     VARCHAR (4000) ,
    geo_enabled     integer,
    lang            VARCHAR (4000) ,
    followers_count integer ,
    favourites_count integer ,
    friends_count   integer ,
    listed_count   integer ,
    loca            VARCHAR (4000),
    status_id bigint REFERENCES T_Status(ID),
    statuses_count        integer ,
    TimeZone varchar(4000),
    user_URL varchar(4000),
    URLEntity_id bigint,
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
ALTER TABLE T_Status ADD FOREIGN KEY (status_user_id,recorded_at) REFERENCES T_User(ID,recorded_at);

CREATE  TABLE T_URL
(
    ID bigint PRIMARY KEY,
    display_url   VARCHAR (4000) ,
    expanded_url  VARCHAR (4000) ,
    indices_start integer ,
    indices_end   integer ,
    url           VARCHAR (4000),
    urltext	  VARCHAR (4000),
    entity_id	  bigint REFERENCES T_Entity(ID),
    descURL_user_id   bigint REFERENCES T_User(ID)
);

ALTER TABLE T_User ADD FOREIGN KEY (URLEntity_id) REFERENCES T_URL(ID);


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




