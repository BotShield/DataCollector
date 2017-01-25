package de.botshield;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
        Statement st = null;
        try {
            /*
             * // hole status_id Statement st = db.createStatement(); ResultSet
             * rs = st.executeQuery("select nextval('status_seq')"); rs.next();
             * long lstatusid=rs.getLong(1); rs.close(); st.close();
             */
            // schreibe status-objekt

            st = db.createStatement();
            st.executeUpdate("insert into T_Status(ID,status_Text) values ("
                    + Long.toString(twStatus.getId()) + ",'"
                    + twStatus.getText() + "')");
            st.close();

            db.commit();

            return 0;
        } catch (SQLException e) {
            System.err.println(e.toString());
            if (db != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    db.rollback();
                    if (st != null)
                        st.close();
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
