/*
 * Copyright 2007 Yusuke Yamamoto
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.botshield;

import java.sql.SQLException;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Klasse zum Schreiben von Tweets in eine DB basierend auf Schlagwoertern.
 */
public final class CaptureFilterStream {

    private PGDBConnection dbConn;
    private boolean blnWritetoDB;
    private StatusListener listener;

    /*
     * Constructor
     */
    public CaptureFilterStream() {
    }

    public void initializeStreamListener() {

        listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (isBlnWritetoDB()) {
                    System.out.println("*********************** Writing tweet into db! *********************** ");
                    dbConn.insertStatus(status);
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.err.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.err.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.err.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.err.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                // TODO: sauberer Abbau der DB Connection bei Programmende
                // hier kein closeConnection, da jede SQLException sonst alle
                // zukuenftigen Schreibprozess unmoeglich macht
                // dbConn.closeConnection();
                ex.printStackTrace();
            }
        };

    }

    /**
     * @return false if connection could not be established
     * @throws SQLException
     */
    public boolean setupConnection() throws SQLException {
        dbConn = new PGDBConnection();
        boolean result = dbConn.establishConnection("dbUser", "dataCollector", "twitter");
        if (result) {
            dbConn.prepareStatements();
        }
        return result;
    }

    public void execute(long[] followArray, String[] trackArray) {
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);

        // filter() method internally creates a thread which manipulates
        // TwitterStream and calls these adequate listener methods continuously.
        twitterStream.filter(new FilterQuery(0, followArray, trackArray));
    }

    public boolean isBlnWritetoDB() {
        return blnWritetoDB;
    }

    public void setBlnWritetoDB(boolean blnWritetoDB) {
        this.blnWritetoDB = blnWritetoDB;
    }

}
