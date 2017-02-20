package de.botshield;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;

/**
 * Klasse zum Schreiben des Twitter Datenmodells in eine relationale Datenbank
 * (Schema scripts/tabellendefinitionen.sql)
 *
 * @author jstrebel
 *
 */

public class PGDBConnection {
    private Connection db = null;

    /**
     * Establishes a DB connection to a PostgreSQL DB.
     *
     * @param strUser
     *            the username
     * @param strPW
     *            the password
     * @param strDB
     *            the DB name
     * @return 0 for success, -1 for failure
     */
    public int establishConnection(String strUser, String strPW, String strDB) {
        String url = "jdbc:postgresql://localhost/" + strDB;
        Statement st;

        try {
            Class.forName("org.postgresql.Driver");
            db = DriverManager.getConnection(url, strUser, strPW);

            st = db.createStatement();
            // ResultSet rs = st.executeQuery("SELECT * FROM \"T_TW_USERS\"");
            ResultSet rs = st.executeQuery("SELECT * FROM T_User");
            System.out.println(rs.toString());

            rs.close();
            st.close();

            db.setAutoCommit(false);

            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return -1;
        }
    }// establishConnection

    /**
     * Method for writing the data collection parameters to the database. It
     * creates a single entry for each collection session.
     *
     * @param strTopics
     *            the topics used for filtering
     * @param strDatasource
     *            REST or Streaming API
     * @return parameter ID
     */
    public int registerDataCollectionParameter(String strTopics,
            String strDatasource) {
        PreparedStatement stInsDCPARAM = null;
        String strInsDCPARAM = "insert into T_DataCollParameter(ID,track_topics,datasource) "
                + "values (?,?,?)";
        try {
            stInsDCPARAM = db.prepareStatement(strInsDCPARAM);
            /*
             * // hole Sequenznummer für die Session Statement st =
             * db.createStatement(); ResultSet rs =
             * st.executeQuery("select nextval('param_seq')"); rs.next(); long
             * lPARAMid = rs.getLong(1); rs.close(); st.close();
             *
             * stInsDCPARAM.setLong(1, lPARAMid); stInsDCPARAM.setString(2,
             * strTopics);
             *
             * stInsDCPARAM.executeUpdate(); db.commit(); stInsDCPARAM.close();
             */
            // TODO: associate Status with param ID
            return 0;
        } catch (SQLException e) {
            /*
             * System.err.println(e.toString()); if (db != null) { try {
             * System.err.print("Transaction is being rolled back");
             * db.rollback(); if (stInsDCPARAM != null) stInsDCPARAM.close(); }
             * catch (SQLException excep) {
             * System.err.println(excep.toString()); } } // if
             */
            return -1;
        }
    }

    /**
     * Inserts Status object into database.
     *
     * @param twStatus
     *            The Twitter4J Status interface
     * @return 0 for success, -1 for failure
     */
    public int insertStatus(Status twStatus) {
        PreparedStatement stInsStatus = null;
        String strInsStatus = "insert into T_Status(ID,status_Text,created_at, "
                + "favourites_count,username,screen_name,lang,withheld_in_countries,"
                + "InReplyToScreenName,InReplyToStatusId,"
                + "InReplyToUserId,quoted_status_id,RetweetCount,retweeted_status_id,status_source,isFavorited,"
                + "isPossiblySensitive,isRetweet,isRetweeted,isRetweetedByMe,isTruncated,recorded_at,status_user_id,latitude,longitude,status_place_id) "
                + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        PreparedStatement stInsUser = null;
        String strInsUser = "insert into T_User(ID,recorded_at,"
                + "username,screen_name,created_at,description,"
                + "geo_enabled,lang,followers_count,favourites_count,friends_count, listed_count,loca,"
                + "statuses_count,TimeZone,user_URL,UtcOffset,WithheldInCountries,isContributorsEnabled,"
                + "isDefaultProfile,isDefaultProfileImage,isFollowRequestSent,isProfileBackgroundTiled,"
                + "isProfileUseBackgroundImage,isProtected,isTranslator,isverified,URLEntity_id,DescURLEntity_id) "
                + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        PreparedStatement stInsURL = null;
        String strInsURL = "insert into T_URL(ID,display_url,expanded_url,"
                + "indices_start,indices_end,url,urltext,entity_id) values (?,?,?,?,?,?,?,?)";

        PreparedStatement stInsPlace = null;
        String strInsPlace = "insert into T_Place(ID,pname,pfullname,place_url,bb_type,geo_type,country,country_code,"
                + "place_type,street_address,contained_place_id) values (?,?,?,?,?,?,?,?,?,?,?)";

        PreparedStatement stInsEntity = null;
        String strInsEntity = "insert into T_Entity(ID) values (?)";

        /*
         * Code aus
         * twitter4j/twitter4j-core/src/internal-json/java/twitter4j/JSONImplFactory
         * .java, um zu verstehen, wie die das zweidimensionale
         * GeoLocation-Array der BoundingBox in Place aufgebaut ist
         *
         * static GeoLocation[][] coordinatesAsGeoLocationArray(JSONArray
         * coordinates) throws TwitterException { try { GeoLocation[][]
         * boundingBox = new GeoLocation[coordinates.length()][]; for (int i =
         * 0; i < coordinates.length(); i++) { JSONArray array =
         * coordinates.getJSONArray(i); boundingBox[i] = new
         * GeoLocation[array.length()]; for (int j = 0; j < array.length(); j++)
         * { JSONArray coordinate = array.getJSONArray(j); boundingBox[i][j] =
         * new GeoLocation(coordinate.getDouble(1), coordinate.getDouble(0)); }
         * } return boundingBox; } catch (JSONException jsone) { throw new
         * TwitterException(jsone); } }
         */
        PreparedStatement stInsGeoLoc = null;
        String strInsGeoLoc = "insert into T_Geolocation(ID,latitude, longitude, bboxcoord_place_id, geocoord_place_id)"
                + " values (?,?,?,?,?)";

        Timestamp tsrecorded_at = new Timestamp(Calendar.getInstance()
                .getTimeInMillis());

        long lURLEntityid = -1;
        long lDescURLEntityid = -1; // User Description URL Entitites
        long lPlaceid = -1;
        long lGeoLocid = -1;

        try {

            // Schreibe Place Objekt
            if (twStatus.getPlace() != null) {
                // TODO: mache db.prepareStatement() nur einmal pro Statement,
                // nicht bei jedem Methodenaufruf
                stInsPlace = db.prepareStatement(strInsPlace);

                Place twPlace = null;
                twPlace = twStatus.getPlace();

                // hole Sequenznummer für den Place
                Statement st = db.createStatement();
                ResultSet rs = st.executeQuery("select nextval('place_seq')");
                rs.next();
                lPlaceid = rs.getLong(1);
                rs.close();
                st.close();
                // setze Parameter
                stInsPlace.setLong(1, lPlaceid);
                stInsPlace.setString(2, twPlace.getName());
                stInsPlace.setString(3, twPlace.getFullName());
                stInsPlace.setString(4, twPlace.getURL());
                stInsPlace.setString(5, twPlace.getBoundingBoxType());
                stInsPlace.setString(6, twPlace.getGeometryType());
                stInsPlace.setString(7, twPlace.getCountry());
                stInsPlace.setString(8, twPlace.getCountryCode());
                stInsPlace.setString(9, twPlace.getPlaceType());
                stInsPlace.setString(10, twPlace.getStreetAddress());
                // TODO: rekursive Auflösung verschachtelter Places
                // nicht wichtig, da Place sowieso nicht oft vorkommt
                stInsPlace.setNull(11, Types.BIGINT);

                stInsPlace.executeUpdate();

                // schreibe BoundingBox aus Place
                // Durchlaufe das doppelte Array GeoLocation[0][j] und schreibe
                // die Einzelkoordinaten in die DB
                // Annahme: es gibt nur ein einziges Polygon als Bounding Box
                GeoLocation[][] arrGeo = null;
                arrGeo = twPlace.getBoundingBoxCoordinates();

                if (arrGeo != null) {
                    GeoLocation[] arrbboxloc = arrGeo[0];
                    Statement st1 = db.createStatement();
                    stInsGeoLoc = db.prepareStatement(strInsGeoLoc);

                    // Durchlaufe alle Ecken des Polygons
                    for (GeoLocation element : arrbboxloc) {
                        // hole id aus Sequence geoloc_seq
                        ResultSet rs1 = st1
                                .executeQuery("select nextval('geoloc_seq')");
                        rs1.next();
                        lGeoLocid = rs1.getLong(1);
                        rs1.close();

                        // schreibe Attribute
                        // ID,latitude, longitude, bboxcoord_place_id,
                        // geocoord_place_id
                        stInsGeoLoc.setLong(1, lGeoLocid);
                        stInsGeoLoc.setDouble(2, element.getLatitude());
                        stInsGeoLoc.setDouble(3, element.getLongitude());
                        stInsGeoLoc.setLong(4, lPlaceid);
                        stInsGeoLoc.setNull(5, Types.BIGINT);
                        stInsGeoLoc.executeUpdate();
                    }// for
                    st1.close();
                    stInsGeoLoc.close();
                }// if

                // schreibe Geometry aus Place
                arrGeo = null;
                arrGeo = twPlace.getGeometryCoordinates();

                if (arrGeo != null) {
                    GeoLocation[] arrbboxloc = arrGeo[0];
                    Statement st1 = db.createStatement();
                    stInsGeoLoc = db.prepareStatement(strInsGeoLoc);

                    // Durchlaufe alle Ecken des Polygons
                    for (GeoLocation element : arrbboxloc) {
                        // hole id aus Sequence geoloc_seq
                        ResultSet rs1 = st1
                                .executeQuery("select nextval('geoloc_seq')");
                        rs1.next();
                        lGeoLocid = rs1.getLong(1);
                        rs1.close();

                        // schreibe Attribute
                        // ID,latitude, longitude, bboxcoord_place_id,
                        // geocoord_place_id
                        stInsGeoLoc.setLong(1, lGeoLocid);
                        stInsGeoLoc.setDouble(2, element.getLatitude());
                        stInsGeoLoc.setDouble(3, element.getLongitude());
                        stInsGeoLoc.setNull(4, Types.BIGINT);
                        stInsGeoLoc.setLong(5, lPlaceid);
                        stInsGeoLoc.executeUpdate();
                    }// for
                    st1.close();
                    stInsGeoLoc.close();
                }// if arrgeo != null

            }// Place != null

            // schreibe URL-Objekt
            if (twStatus.getUser() != null)

                // User's URL
                // leider kann es das URLEntity-Objekt geben, ohne dass dessen
                // Felder gefüllt sind.
                if (twStatus.getUser().getURLEntity() != null)
                    if (!twStatus.getUser().getURLEntity().getURL().isEmpty()) {
                        stInsURL = db.prepareStatement(strInsURL);
                        stInsEntity = db.prepareStatement(strInsEntity);

                        URLEntity twURL = null;
                        long lURLid = -1;

                        twURL = twStatus.getUser().getURLEntity();

                        // hole Sequenznummer für die URL und die Entity
                        Statement st = db.createStatement();
                        ResultSet rs = st
                                .executeQuery("select nextval('url_seq'),nextval('entity_seq')");
                        rs.next();
                        lURLid = rs.getLong(1);
                        lURLEntityid = rs.getLong(2);
                        rs.close();
                        st.close();

                        // schreibe Entity zuerst
                        stInsEntity.setLong(1, lURLEntityid);
                        stInsEntity.executeUpdate();

                        // dann schreibe URL
                        stInsURL.setLong(1, lURLid);
                        stInsURL.setString(2, twURL.getDisplayURL());
                        stInsURL.setString(3, twURL.getExpandedURL());
                        stInsURL.setInt(4, twURL.getStart());
                        stInsURL.setInt(5, twURL.getEnd());
                        stInsURL.setString(6, twURL.getURL());
                        stInsURL.setString(7, twURL.getText());
                        stInsURL.setLong(8, lURLEntityid);
                        stInsURL.executeUpdate();
                        stInsURL.close();
                        stInsEntity.close();
                    } // if getURLEntity!=null

            // User Description URL Entities
            if (twStatus.getUser().getDescriptionURLEntities() != null) {
                stInsURL = db.prepareStatement(strInsURL);
                stInsEntity = db.prepareStatement(strInsEntity);

                URLEntity[] arrURL = null;
                long lURLid = -1;

                arrURL = twStatus.getUser().getDescriptionURLEntities();

                // get Entity ID
                Statement st = db.createStatement();
                ResultSet rs = st.executeQuery("select nextval('entity_seq')");
                rs.next();
                lDescURLEntityid = rs.getLong(1);
                rs.close();
                st.close();

                // schreibe Entity zuerst
                stInsEntity.setLong(1, lDescURLEntityid);
                stInsEntity.executeUpdate();

                for (URLEntity elem : arrURL) {
                    // hole Sequenznummer für die URL und die Entity
                    st = db.createStatement();
                    rs = st.executeQuery("select nextval('url_seq')");
                    rs.next();
                    lURLid = rs.getLong(1);
                    rs.close();
                    st.close();

                    // dann schreibe URL
                    stInsURL.setLong(1, lURLid);
                    stInsURL.setString(2, elem.getDisplayURL());
                    stInsURL.setString(3, elem.getExpandedURL());
                    stInsURL.setInt(4, elem.getStart());
                    stInsURL.setInt(5, elem.getEnd());
                    stInsURL.setString(6, elem.getURL());
                    stInsURL.setString(7, elem.getText());
                    stInsURL.setLong(8, lDescURLEntityid);
                    stInsURL.executeUpdate();

                } // for

                stInsURL.close();
                stInsEntity.close();
            } // if getDescriptionURLEntities!=null

            // schreibe User-Objekt
            stInsUser = db.prepareStatement(strInsUser);
            User twUser = null;
            if (twStatus.getUser() != null) {
                twUser = twStatus.getUser();

                stInsUser.setLong(1, twUser.getId());
                stInsUser.setTimestamp(2, tsrecorded_at);
                stInsUser.setString(3, twUser.getName());
                stInsUser.setString(4, twUser.getScreenName());
                stInsUser.setTimestamp(5, new Timestamp(twUser.getCreatedAt()
                        .getTime()));
                stInsUser.setString(6, twUser.getDescription());
                stInsUser.setInt(7, (twUser.isGeoEnabled() ? 1 : 0));
                stInsUser.setString(8, twUser.getLang());
                stInsUser.setInt(9, twUser.getFollowersCount());
                stInsUser.setInt(10, twUser.getFavouritesCount());
                stInsUser.setInt(11, twUser.getFriendsCount());
                stInsUser.setInt(12, twUser.getListedCount());
                stInsUser.setString(13, twUser.getLocation());
                stInsUser.setInt(14, twUser.getStatusesCount());
                stInsUser.setString(15, twUser.getTimeZone());
                stInsUser.setString(16, twUser.getURL());

                stInsUser.setInt(17, twUser.getUtcOffset());

                if (twUser.getWithheldInCountries() != null) {
                    stInsUser.setString(18, twUser.getWithheldInCountries()
                            .toString());
                } else {
                    stInsUser.setNull(18, Types.VARCHAR);
                }
                ;
                stInsUser.setInt(19, (twUser.isContributorsEnabled() ? 1 : 0));
                stInsUser.setInt(20, (twUser.isDefaultProfile() ? 1 : 0));
                stInsUser.setInt(21, (twUser.isDefaultProfileImage() ? 1 : 0));
                stInsUser.setInt(22, (twUser.isFollowRequestSent() ? 1 : 0));
                stInsUser.setInt(23,
                        (twUser.isProfileBackgroundTiled() ? 1 : 0));
                stInsUser.setInt(24, (twUser.isProfileUseBackgroundImage() ? 1
                        : 0));
                stInsUser.setInt(25, (twUser.isProtected() ? 1 : 0));
                stInsUser.setInt(26, (twUser.isTranslator() ? 1 : 0));
                stInsUser.setInt(27, (twUser.isVerified() ? 1 : 0));

                if (lURLEntityid != -1) {
                    stInsUser.setLong(28, lURLEntityid);
                } else {
                    stInsUser.setNull(28, Types.BIGINT);
                }

                if (lDescURLEntityid != -1) {
                    stInsUser.setLong(29, lDescURLEntityid);
                } else {
                    stInsUser.setNull(29, Types.BIGINT);
                }

                stInsUser.executeUpdate();
                stInsUser.close();
            }

            // schreibe Status-objekt

            stInsStatus = db.prepareStatement(strInsStatus);
            stInsStatus.setLong(1, twStatus.getId());
            stInsStatus.setString(2, twStatus.getText());
            stInsStatus.setTimestamp(3, new Timestamp(twStatus.getCreatedAt()
                    .getTime()));
            stInsStatus.setInt(4, twStatus.getFavoriteCount());
            stInsStatus.setString(5, twStatus.getUser().getName());
            stInsStatus.setString(6, twStatus.getUser().getScreenName());
            stInsStatus.setString(7, twStatus.getLang());

            if (twStatus.getWithheldInCountries() != null) {
                stInsStatus.setString(8, twStatus.getWithheldInCountries()
                        .toString());
            } else {
                stInsStatus.setNull(8, Types.VARCHAR);
            }
            ;
            stInsStatus.setString(9, twStatus.getInReplyToScreenName());
            stInsStatus.setLong(10, twStatus.getInReplyToStatusId());
            stInsStatus.setLong(11, twStatus.getInReplyToUserId());
            stInsStatus.setLong(12, twStatus.getQuotedStatusId());
            stInsStatus.setInt(13, twStatus.getRetweetCount());
            if (twStatus.getRetweetedStatus() != null) {
                stInsStatus.setLong(14, twStatus.getRetweetedStatus().getId());
            } else {
                stInsStatus.setNull(14, Types.BIGINT);
            }
            stInsStatus.setString(15, twStatus.getSource());
            stInsStatus.setInt(16, (twStatus.isFavorited() ? 1 : 0));
            stInsStatus.setInt(17, (twStatus.isPossiblySensitive() ? 1 : 0));
            stInsStatus.setInt(18, (twStatus.isRetweet() ? 1 : 0));
            stInsStatus.setInt(19, (twStatus.isRetweeted() ? 1 : 0));
            stInsStatus.setInt(20, (twStatus.isRetweetedByMe() ? 1 : 0));
            stInsStatus.setInt(21, (twStatus.isTruncated() ? 1 : 0));
            stInsStatus.setTimestamp(22, tsrecorded_at);

            if (twStatus.getUser() != null) {
                stInsStatus.setLong(23, twStatus.getUser().getId());
            } else {
                stInsStatus.setNull(23, Types.BIGINT);
            }

            // Geolocation des Tweets
            if (twStatus.getGeoLocation() != null) {
                stInsStatus.setDouble(24, twStatus.getGeoLocation()
                        .getLatitude());
                stInsStatus.setDouble(25, twStatus.getGeoLocation()
                        .getLongitude());
            } else {
                stInsStatus.setNull(24, Types.DOUBLE);
                stInsStatus.setNull(25, Types.DOUBLE);
            }
            // Place
            if (lPlaceid != -1) {
                stInsStatus.setLong(26, lPlaceid);
            } else {
                stInsStatus.setNull(26, Types.BIGINT);
            }

            // TODO: getExtendedMediaEntities, getHashtagEntities,
            // getMediaEntities, getSymbolEntities, getURLEntities,
            // getUserMentionEntities

            stInsStatus.executeUpdate();

            db.commit();
            if (stInsStatus != null)
                stInsStatus.close();
            if (stInsURL != null)
                stInsURL.close();
            if (stInsPlace != null)
                stInsPlace.close();
            return 0;
        } catch (SQLException e) {
            // System.err.println(e.toString());
            e.printStackTrace();
            System.err.println(e.getSQLState());
            if (db != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    db.rollback();
                    if (stInsStatus != null)
                        stInsStatus.close();
                    if (stInsUser != null)
                        stInsUser.close();
                    if (stInsURL != null)
                        stInsURL.close();
                    if (stInsPlace != null)
                        stInsPlace.close();
                } catch (SQLException excep) {
                    System.err.println(excep.toString());
                }
            } // if
            return -1;
        }// catch
    }// insertStatus

    public int closeConnection() {
        try {
            db.close();
            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return -1;
        }
    }// closeConnection

}// class
