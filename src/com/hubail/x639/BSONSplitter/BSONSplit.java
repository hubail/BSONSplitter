package com.hubail.x639.BSONSplitter;
/**
 * Splits large BSON dumps into "chunks" of smaller BSONs
 * @author hubail
 */
public class BSONSplit {

    /**
     * TODO
     */
    String mongoPath;
    String mongoDatabaseName;
    String mongoCollectionName;

    public static void main(String[] args){
        try {
            if (args.length != 3){
                System.out.println("Usage: BSONSplit source.bson 100 C:\\output_folder");
            } else {
                new BSONSplitter(args[0], Integer.valueOf(args[1]), args[2]);
            }
        } catch (Exception e) {
            System.out.println("An error has been occured: "+e.getMessage());
        }
    }

}
