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

import twitter4j.Place;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;

/**
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
            // TODO: aaociate Status with param ID
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
     * Vorgehen beim Schreiben des Status: 1. T_Geolocation schreiben --> führt
     * evtl. dazu, me 2. T_Place schreiben 3. User schreiben 4. Status schreiben
     * Das ganze sollte in einer Transaktionsklammer passieren, um bei Fehlern
     * keine Inkonsistenzen in den Daten zu erzeugen
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
                + "isProfileUseBackgroundImage,isProtected,isTranslator,isverified,URLEntity_id) "
                + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        PreparedStatement stInsURL = null;
        String strInsURL = "insert into T_URL(ID,recorded_at,display_url,expanded_url,"
                + "indices_start,indices_end,url,urltext,entity_id) values (?,?,?,?,?,?,?,?,?)";

        PreparedStatement stInsPlace = null;
        String strInsPlace = "insert into T_Place(ID,pname,pfullname,place_url,bb_type,geo_type,country,country_code,"
                + "place_type,street_address,contained_place_id) values (?,?,?,?,?,?,?,?,?,?,?)";

        Timestamp tsrecorded_at = new Timestamp(Calendar.getInstance()
                .getTimeInMillis());
        long lURLid = -1;
        long lPlaceid = -1;

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
                stInsPlace.setNull(11, Types.BIGINT);

                stInsPlace.executeUpdate();
            }

            // schreibe URL-Objekt
            if (twStatus.getUser() != null)
                if (twStatus.getUser().getURLEntity() != null) {
                    // TODO: mache db.prepareStatement() nur einmal pro
                    // Statement,
                    // nicht bei jedem Methodenaufruf
                    stInsURL = db.prepareStatement(strInsURL);
                    URLEntity twURL = null;
                    twURL = twStatus.getUser().getURLEntity();

                    // hole Sequenznummer für die URL
                    Statement st = db.createStatement();
                    ResultSet rs = st.executeQuery("select nextval('url_seq')");
                    rs.next();
                    lURLid = rs.getLong(1);
                    rs.close();
                    st.close();
                    // setze Parameter
                    stInsURL.setLong(1, lURLid);
                    stInsURL.setTimestamp(2, tsrecorded_at);
                    stInsURL.setString(3, twURL.getDisplayURL());
                    stInsURL.setString(4, twURL.getExpandedURL());
                    stInsURL.setInt(5, twURL.getStart());
                    stInsURL.setInt(6, twURL.getEnd());
                    stInsURL.setString(7, twURL.getURL());
                    stInsURL.setString(8, twURL.getText());
                    stInsURL.setNull(9, Types.BIGINT); // TODO: entity_id bigint
                    // noch zu setzen, wenn
                    // Entities implementiert
                    // sind.
                    // REFERENCES T_Entity(ID)
                    stInsURL.executeUpdate();
                }

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
                if (lURLid != -1) {
                    stInsUser.setLong(28, lURLid);
                } else {
                    stInsUser.setNull(28, Types.BIGINT);
                }
                stInsUser.executeUpdate();
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

            stInsStatus.executeUpdate();

            db.commit();
            if (stInsStatus != null)
                stInsStatus.close();
            if (stInsUser != null)
                stInsUser.close();
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
