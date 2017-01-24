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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Klasse zum Schreiben von Tweets in eine DB basierend auf Schlagwörtern.
 */
public final class CaptureFilterStream {

    private PGDBConnection dbConn;
    private boolean blnWritetoDB = false;
    private final static String PROPERTY_FILE = "dataCollector.properties";
    private final static String PROPERTY_KEY_FOLLOW = "toBeFollowed";
    private final static String PROPERTY_KEY_TOPICS = "trackTopics";
    private final static String PROPERTY_SEPARATOR = ",";
    private final static String PROPERTY_DATABASEINTEGRATION = "WriteToDatabase";

    StatusListener listener = new StatusListener() {
        @Override
        public void onStatus(Status status) {
            System.out.println("@" + status.getUser().getScreenName() + " - "
                    + status.getText());
            if (blnWritetoDB)
                dbConn.insertStatus(status);
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            System.err.println("Got a status deletion notice id:"
                    + statusDeletionNotice.getStatusId());
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            System.err.println("Got track limitation notice:"
                    + numberOfLimitedStatuses);
        }

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {
            System.err.println("Got scrub_geo event userId:" + userId
                    + " upToStatusId:" + upToStatusId);
        }

        @Override
        public void onStallWarning(StallWarning warning) {
            System.err.println("Got stall warning:" + warning);
        }

        @Override
        public void onException(Exception ex) {
            dbConn.closeConnection();
            ex.printStackTrace();
        }
    };

    /*
     * Constructor
     */
    public CaptureFilterStream() {

        /*
         * Alternative Konfigurationsmethode ohne Properties-Datei
         * ConfigurationBuilder cb = new ConfigurationBuilder();
         * cb.setDebugEnabled(true)
         * .setOAuthConsumerKey("*********************")
         * .setOAuthConsumerSecret("******************************************")
         * .setOAuthAccessToken(
         * "**************************************************")
         * .setOAuthAccessTokenSecret(
         * "******************************************");
         *
         * TwitterFactory tf = new TwitterFactory(cb.build()); Twitter twitter =
         * tf.getInstance();
         */
    }

    /**
     * @return -1 if connection could not be established
     */
    public int setupConnection() {
        dbConn = new PGDBConnection();
        return dbConn.establishConnection("jstrebel", "", "twitter"); // login
        // meiner
        // Testdatenbank
        // auf localhost
    }

    public void execute(long[] followArray, String[] trackArray)
            throws TwitterException {
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);

        // filter() method internally creates a thread which manipulates
        // TwitterStream and calls these adequate listener methods continuously.
        twitterStream.filter(new FilterQuery(0, followArray, trackArray));
    }

    public static void main(String[] args) throws TwitterException {
        Properties props = new Properties();
        CaptureFilterStream objCapture = new CaptureFilterStream();

        // Read definitions from property file
        try {
            InputStream is = objCapture.getClass().getClassLoader()
                    .getResourceAsStream(PROPERTY_FILE);
            props.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        long[] followArray = readFollow(props);
        String[] trackArray = readTopics(props);

        // Check if database integration is needed
        if (props.getProperty(PROPERTY_DATABASEINTEGRATION).trim()
                .equals("true")
                && objCapture.setupConnection() != -1) {
            objCapture.setBlnWritetoDB(true);
        } else {
            objCapture.setBlnWritetoDB(false);
        }

        // start following
        objCapture.execute(followArray, trackArray);
    }

    /**
     * Extracts the topics that are to be tracked from a property file. The
     * topics are expected to be contained in the passed "props" as a
     * comma-separated list of Strings.
     *
     * @param props
     *            Contains the topics to be tracked.
     * @return The topics to be tracked as an array of String.
     */
    private static String[] readTopics(Properties props) {
        String[] trackArray = null;
        String topicsToBeTracked = props.getProperty(PROPERTY_KEY_TOPICS);
        if (topicsToBeTracked != null && !topicsToBeTracked.trim().isEmpty()) {
            String[] topicsArray = topicsToBeTracked.split(PROPERTY_SEPARATOR);

            // the track array may contain a maximum of 10 entries TODO: stimmt
            // das?
            trackArray = new String[Math.min(10, topicsArray.length)];
            for (int n = 0; n < topicsArray.length; n++) {
                String topicToBeTracked = topicsArray[n].trim();
                trackArray[n] = topicToBeTracked.trim();
            }

        }
        return trackArray;
    }

    /**
     * Extracts the users that are to be followed from a property file. The
     * users are expected to be contained in the passed "props" as
     * comma-separated list of longs.
     *
     * @param props
     *            Contains the users to be followed.
     * @return The users to be followed as an array of long.
     */
    private static long[] readFollow(Properties props) {
        long[] followArray = null;
        String usersToBeFollowed = props.getProperty(PROPERTY_KEY_FOLLOW);
        if (usersToBeFollowed != null && !usersToBeFollowed.trim().isEmpty()) {
            String[] usersArray = usersToBeFollowed.split(PROPERTY_SEPARATOR);
            // the follow array may contain a maximum of 10 entries TODO: stimmt
            // das?
            followArray = new long[Math.min(10, usersArray.length)];
            for (int n = 0; n < followArray.length; n++) {
                String userToBeFollowed = usersArray[n].trim();
                followArray[n] = Long.parseLong(userToBeFollowed);
            }
        }
        return followArray;
    }

    public boolean isBlnWritetoDB() {
        return blnWritetoDB;
    }

    public void setBlnWritetoDB(boolean blnWritetoDB) {
        this.blnWritetoDB = blnWritetoDB;
    }

}
