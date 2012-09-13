package com.hubail.x639.BSONSplitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import com.mongodb.DBCollection;
import com.mongodb.LazyDBDecoder;
import com.mongodb.LazyDBObject;

public class BSONSplitter {

    InputStream in;
    OutputStream out;
    LazyDBDecoder decoder;
    LazyDBObject object;
    
    /**
     * Splits large BSON dumps into "chunks" of smaller BSONs
     * Based on "Scott Hernandez" code from the "mongodb-user" mailing list
     * 
     * @param filename Source BSON file
     * @param _chunkSize Each part's size limit in MB
     * @param outputDir Writable directory to create parts into
     * @throws Exception
     */
    public BSONSplitter(String filename, int _chunkSize, String outputDir) throws Exception {
        long chunkSize = _chunkSize * 1024L * 1024L;
        long chunkTotal = 0;
        int partNo = 0;
        
        in = new FileInputStream(filename);
        out = new FileOutputStream(outputDir+File.separator+partNo+".bson");
        decoder = new LazyDBDecoder();       
        
        System.out.println("Started splitting "+filename+" into chunks of "+_chunkSize+" MB each");
        System.out.println("Working directory: "+outputDir);
        
        while (in.available() > 0){
            object = (LazyDBObject) decoder.decode(in, (DBCollection) null);
            chunkTotal += object.getBSONSize();
            
            object.pipe(out);

            if (chunkTotal > chunkSize) {
                out.close();
                out = new FileOutputStream(outputDir+File.separator+partNo+".bson");
                
                System.out.println("Created "+partNo+".bson");
                
                chunkTotal = 0;
                partNo++;
            }
        }
        
        if (out != null) out.close();
    }
}
