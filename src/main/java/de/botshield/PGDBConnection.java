package de.botshield;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;

import twitter4j.Status;

public class PGDBConnection {
    private Connection db;

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

    /*
     * Vorgehen beim Schreiben des Status: 1. T_Geolocation schreiben --> führt
     * evtl. dazu, me 2. T_Place schreiben 3. User schreiben 4. Status schreiben
     * Das ganze sollte in einer Transaktionsklammer passieren, um bei Fehlern
     * keine Inkonsistenzen in den Daten zu erzeugen
     */
    public int insertStatus(Status twStatus) {
        PreparedStatement stInsStatus = null;
        String strInsStatus = "insert into T_Status(ID,status_Text,created_at, "
                + "favourites_count,username,screen_name,lang,withheld_in_countries,InReplyToScreenName,InReplyToStatusId,"
                + "InReplyToUserId,quoted_status_id,RetweetCount,retweeted_status_id,status_source,isFavorited,"
                + "isPossiblySensitive,isRetweet,isRetweeted,isRetweetedByMe,isTruncated,recorded_at) "
                + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        PreparedStatement stInsUser = null;
        String strInsUser = "insert into T_User(ID,recorded_at) "
                + "values (?,?)";
        Timestamp tsrecorded_at = new Timestamp(Calendar.getInstance()
                .getTimeInMillis());

        try {
            /*
             * // hole status_id Statement st = db.createStatement(); ResultSet
             * rs = st.executeQuery("select nextval('status_seq')"); rs.next();
             * long lstatusid=rs.getLong(1); rs.close(); st.close();
             */

            // schreibe User-Objekt
            stInsUser = db.prepareStatement(strInsUser);
            stInsUser.setLong(1, twStatus.getId());
            stInsUser.setTimestamp(2, tsrecorded_at);

            // schreibe Status-objekt

            stInsStatus = db.prepareStatement(strInsStatus);
            stInsStatus.setLong(1, twStatus.getId());
            stInsStatus.setString(2, twStatus.getText());
            stInsStatus.setTimestamp(3, new Timestamp(twStatus.getCreatedAt()
                    .getTime()));
            stInsStatus.setInt(4, twStatus.getFavoriteCount());
            // geoloc_id bigint REFERENCES T_Geolocation(ID),
            stInsStatus.setString(5, twStatus.getUser().getName());
            stInsStatus.setString(6, twStatus.getUser().getScreenName());
            stInsStatus.setString(7, twStatus.getLang());
            // status_place_id bigint REFERENCES T_Place(ID)
            stInsStatus.setString(8, twStatus.getWithheldInCountries()
                    .toString());
            stInsStatus.setString(9, twStatus.getInReplyToScreenName());
            stInsStatus.setLong(10, twStatus.getInReplyToStatusId());
            stInsStatus.setLong(11, twStatus.getInReplyToUserId());
            stInsStatus.setLong(12, twStatus.getQuotedStatusId());
            stInsStatus.setInt(13, twStatus.getRetweetCount());
            stInsStatus.setLong(14, twStatus.getRetweetedStatus().getId());
            stInsStatus.setString(15, twStatus.getSource());
            // status_user_id bigint,--> muss erst User schreiben , um die ID zu
            // haben
            stInsStatus.setInt(16, (twStatus.isFavorited() ? 1 : 0));
            stInsStatus.setInt(17, (twStatus.isPossiblySensitive() ? 1 : 0));
            stInsStatus.setInt(18, (twStatus.isRetweet() ? 1 : 0));
            stInsStatus.setInt(19, (twStatus.isRetweeted() ? 1 : 0));
            stInsStatus.setInt(20, (twStatus.isRetweetedByMe() ? 1 : 0));
            stInsStatus.setInt(21, (twStatus.isTruncated() ? 1 : 0));
            stInsStatus.setTimestamp(22, tsrecorded_at);

            stInsStatus.executeUpdate();
            db.commit();
            stInsStatus.close();
            return 0;
        } catch (SQLException e) {
            System.err.println(e.toString());
            if (db != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    db.rollback();
                    if (stInsStatus != null)
                        stInsStatus.close();
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
