/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trackdrpcquery;

import backtype.storm.utils.DRPCClient;

/**
 *
 * @author jwalton
 */
public class TwitterDrpcQueryThread {
    
    DRPCClient client = new DRPCClient("localhost", 3772);
    
    public void performDrpcQuery() {
        try {
            System.out.println("Performing DRPC TWITTER query");
            String result = client.execute("twitterQuery", "TWITTER_USER");
            System.out.println("Finished");
            /**
             * The trackQuery topology writes the last positions to a pub/sub REDIS service
             */
//            System.out.println("result: "+result);
//            List resultObject = (List)JSON.parse(result);
//            for(int i = 0; i < resultObject.size(); i++){
//                System.out.println("Object at index "+i+": "+resultObject.get(i).toString());
//            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
     public static void main(String[] args) throws Exception{

        TwitterDrpcQueryThread queryThread = new TwitterDrpcQueryThread();
        while (true) {
            queryThread.performDrpcQuery();
            Thread.sleep(600000);
        }
    }
}
