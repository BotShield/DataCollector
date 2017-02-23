package de.botshield;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import twitter4j.TwitterException;

public class Runner {

    private final static String PROPERTY_FILE = "dataCollector.properties";
    private final static String PROPERTY_KEY_FOLLOW = "toBeFollowed";
    private final static String PROPERTY_KEY_TOPICS = "trackTopics";
    private final static String PROPERTY_SEPARATOR = ",";
    private final static String PROPERTY_DATABASEINTEGRATION = "WriteToDatabase";

    public static void main(String[] args) throws TwitterException, SQLException {
        Properties props = new Properties();
        CaptureFilterStream objCapture = new CaptureFilterStream();

        // Read definitions from property file
        try {
            InputStream is = objCapture.getClass().getClassLoader().getResourceAsStream(PROPERTY_FILE);
            props.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        long[] followArray = readFollow(props);
        String[] trackArray = readTopics(props);

        // Check if database integration is needed
        if (props.getProperty(PROPERTY_DATABASEINTEGRATION).trim().equals("true") && objCapture.setupConnection()) {
            objCapture.setBlnWritetoDB(true);
            System.out.println("Write to DB is set to true!");
        } else {
            objCapture.setBlnWritetoDB(false);
            System.out.println("Write to DB is set to false!");
        }

        objCapture.initializeStreamListener();

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

}
