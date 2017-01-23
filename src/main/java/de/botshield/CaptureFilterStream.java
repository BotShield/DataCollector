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

import twitter4j.*;
import twitter4j.conf.*;

/*
 * Klasse zum Schreiben von Tweets in eine DB basierend auf Schlagwörtern. 
 */
public final class CaptureFilterStream 
{
	
	private PGDBConnection dbConn;
    
	StatusListener listener = new StatusListener() 
	{
        @Override
        public void onStatus(Status status) 
        {
            //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
        	dbConn.insertStatus(status);
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
        public void onStallWarning(StallWarning warning) 
        {
            System.err.println("Got stall warning:" + warning);
        }

        @Override
        public void onException(Exception ex) 
        {
        	dbConn.closeConnection();
            ex.printStackTrace();
        }
    };
    
    /*
     * Constructor
     */
    public CaptureFilterStream()
    {
    	dbConn=new PGDBConnection();
    	dbConn.establishConnection("jstrebel", "", "twitter"); //login meiner Testdatenbank auf localhost
    	
    	
    	/* Alternative Konfigurationsmethode ohne Properties-Datei
    	ConfigurationBuilder cb = new ConfigurationBuilder();
    	cb.setDebugEnabled(true)
    	  .setOAuthConsumerKey("*********************")
    	  .setOAuthConsumerSecret("******************************************")
    	  .setOAuthAccessToken("**************************************************")
    	  .setOAuthAccessTokenSecret("******************************************");
    
    	TwitterFactory tf = new TwitterFactory(cb.build());
    	Twitter twitter = tf.getInstance();*/
    }
    
    public void execute(long[] followArray, String[] trackArray) throws TwitterException
    {
    	  TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
          twitterStream.addListener(listener);

          // filter() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
          twitterStream.filter(new FilterQuery(0, followArray, trackArray));
    }
    
	public static void main(String[] args) throws TwitterException 
    {
		//mal als Übergangslösung. Das einfachste wäre eine kleine Textdatei mit Schlagwörtern und Usernamen 
        long[] followArray =null; // = new long[10]; //UserIDs 
        String[] trackArray = new String[1]; //Schlagworte
        
        trackArray[0]="#AfD";
        
        CaptureFilterStream objCapture = new CaptureFilterStream();
        objCapture.execute(followArray, trackArray);
    }

}
