/*
  CS 61C Project1: Small World

  Name: Gregory Roberts
  Login: cs61c-il

  Name: Kevin Funkhouser
  Login: cs61c-as
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum dept for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE};
    
    public static class NodeValue implements Writable {
	public Text name;
	public LongWritable dist;
	public ArrayList<LongWritable> successors;
	public boolean hasTraversed = false;

	public NodeValue(Text n, long d,
			 ArrayList<LongWritable> s) {
	    name = n;
	    dist = new LongWritable(d);
	    successors = s;
	}

	public void write(DataOutput out) throws IOException {
            name.write(out);
	    dist.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    name.set(in.readUTF());
	    dist.set(in.readLong());
        }

        public void setDist(long toWhat) {
	    dist.set(dist.get() + toWhat);
	}

	public void setHasTraversed(boolean toWhat) {
	    hasTraversed = toWhat;
	}

        public String toString() {
            return name.toString() + ": " + dist.get();
        }
    }	
	
    // Example writable type
    public static class EValue implements Writable {
        public ValueUse use;
        public long value;

        public EValue(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public EValue() {
            this(ValueUse.EDGE, 0);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeUTF(use.name());
            out.writeLong(value);
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            use = ValueUse.valueOf(in.readUTF());
            value = in.readLong();
        }

        public void set(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public String toString() {
            return use.name() + ": " + value;
        }
    }


    /* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
        public long denom;

        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
        @Override
        public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
                                        new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }

        /* Will need to modify to not lose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            // Send edge forward only if part of random subset
            if (Math.random() < 1.0/denom) {
                context.write(key, value);
            }
            // Example of using a counter (counter tagged by EDGE)
            context.getCounter(ValueUse.EDGE).increment(1);
        }
    }

    /* Insert your mapreduces here
       (still feel free to edit elsewhere) */
    /* The second mapper . . . */
    /*public static class ProcesserMap extends Mapper<NodeValue, ArrayWritable, NodeValue, LongWritable> {

        public void map(LongWritable key, ArrayWritable value, Context context)
                throws IOException, InterruptedException {
	    if (key.dist() >= 0  && !key.hasTraversed) {
		key.setHasTraversed(true);
		for (LongWritable v : value) {
		    context.write(key, v);
		}
	    }
        }
	}*/
/* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap2 extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
        public long denom;

        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
        @Override
        public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
                                        new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }

        /* Will need to modify to not lose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

	    context.write(key, value);

            // Example of using a counter (counter tagged by EDGE)
            context.getCounter(ValueUse.EDGE).increment(1);
        }
    }
    public static class LoaderReducer extends Reducer<LongWritable, LongWritable, NodeValue, Text> {
	@Override 
        public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context) throws IOException, InterruptedException {
	    ArrayList<LongWritable> mySuccessors = new ArrayList<LongWritable>();
	    Text outputText = new Text();
	    String outputString = "";
	    for (LongWritable value : values) {
		mySuccessors.add(value);
		outputString += value.toString();
	    }
	    outputText.set(outputString);
	    //Object[] s = mySuccessors.toArray();
	    //int size = mySuccessors.size();
	    //LongWritable[] successors = new LongWritable[size];
	    //for (int i = 0; i < size; i += 1) {
	    //	successors[i] = (LongWritable) s[i];
	    //}
	    //ArrayWritable writableSuccessors = new ArrayWritable(org.apache.hadoop.io.LongWritable, successors);
	    Text theName = new Text();
	    theName.set(key.toString());
	    NodeValue newKey = new NodeValue(theName, -1, mySuccessors);
	    context.write(newKey, outputText);
	}
    }
    // Shares denom argument across the cluster via DistributedCache
    public static void shareDenom(String denomStr, Configuration conf) {
        try {
	    Path localDenomPath = new Path(DENOM_PATH + "-source");
	    Path remoteDenomPath = new Path(DENOM_PATH);
	    BufferedWriter writer = new BufferedWriter(
				    new FileWriter(localDenomPath.toString()));
	    writer.write(denomStr);
	    writer.newLine();
	    writer.close();
	    FileSystem fs = FileSystem.get(conf);
	    fs.copyFromLocalFile(true,true,localDenomPath,remoteDenomPath);
	    DistributedCache.addCacheFile(remoteDenomPath.toUri(), conf);
        } catch (IOException ioe) {
            System.err.println("IOException writing to distributed cache");
            System.err.println(ioe.toString());
        }
    }


    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Set denom from command line arguments
        shareDenom(args[2], conf);

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(NodeValue.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LoaderMap2.class);
        job.setReducerClass(LoaderReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Example of reading a counter
        System.out.println("Read in " + 
                   job.getCounters().findCounter(ValueUse.EDGE).getValue() + 
                           " edges");

        // Repeats your BFS mapreduce
        int i=0;
        // Will need to change terminating conditions to respond to data
        while (i<1) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);

            job.setMapperClass(Mapper.class);
            job.setReducerClass(Reducer.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
