package com.hubail.x639.BSONSplitter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import com.mongodb.DBCollection;
import com.mongodb.LazyDBDecoder;
import com.mongodb.LazyDBObject;

public class BSONSplitter {

    InputStream in;
    OutputStream out;
    RandomAccessFile raf;
    FileChannel fc;
    LazyDBDecoder decoder;
    LazyDBObject object;
    
    long chunkTotal = 0;
    int partNo = 1;
    long pointer = 0;
    long chunkSize;
    
    /**
     * Splits large BSON dumps into "chunks" of smaller BSONs
     * 
     * @param filename Source BSON file
     * @param _chunkSize Each part's size limit in MB
     * @param outputDir Writable directory to create parts into
     */
    public BSONSplitter(String filename, int _chunkSize, String outputDir) throws Exception{
        chunkSize = _chunkSize * 1024L * 1024L;
        
        try {
            out = new BufferedOutputStream(new FileOutputStream(outputDir+File.separator+partNo+".bson"));
            raf = new RandomAccessFile(new File(filename), "r");
            fc = raf.getChannel();
            in = Channels.newInputStream(fc);
            decoder = new LazyDBDecoder();
            
            System.out.println("Started splitting "+filename+" into chunks of "+_chunkSize+" MB each");
            System.out.println("Working directory: "+outputDir+"\n");            

            while (fc.position() < fc.size()){                

                object = (LazyDBObject) decoder.decode(in, (DBCollection) null);
                object.pipe(out);
                
                chunkTotal += object.getBSONSize();
                pointer = fc.position();
                
                if (pointer%10007==0) // lucky number ;)
                    System.out.printf("Parsed %2.0f%% of part #%1d\n", ((chunkTotal/(double)chunkSize)*100), partNo);
                
                if (chunkTotal > chunkSize){                    
                    out.close();
                    out = new FileOutputStream(outputDir+File.separator+partNo+".bson");
                    System.out.print("Parsed 100% of part #"+partNo);
                    System.out.println("\n\nCreated "+partNo+".bson\n");
                    
                    chunkTotal = 0;
                    partNo++; 
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException: "+e.getMessage());
        } catch (IOException e) {
            System.out.println("IOException: "+e.getMessage());
        } catch (Exception e) {
            System.out.println("Exception: "+e.getMessage());
            System.out.println("Pointer Location:"+pointer);
        } finally {
            System.out.println("Job completed with "+(partNo)+" parts.");
            if (out != null) out.close();
        }
    }
}
