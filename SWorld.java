/*
  CS 61C Project1: Small World

  Name:
  Login:

  Name:
  Login:
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
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
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


public class SWorld {
    // Maximum dept for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE, SEARCH, DATA};

    // Example writable type
    public static class EValue implements Writable {
        public ValueUse use;
        public long from;
		public long to;
		public String names;
		public String distances;

        public EValue(ValueUse use, long from, long to) {
            this.use = use;
            this.from = from;
			this.to = to;
			names = "";
			distances = "";
        }
		
		public EValue (EValue e) {
			this.use = e.use;
			this.from = e.from;
			this.to = e.to;
			this.names = e.names;
			this.distances = e.distances;
		}	
		
		public EValue(ValueUse use, String d, String n) {
			this.use = use;
			distances = d;
			names = n;
			from = -1;
			to = -1;
		}	
		

        public EValue() {
            this(ValueUse.EDGE, 0, 0);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeUTF(use.name());
            out.writeLong(from);
			out.writeLong(to);
			out.writeUTF(names);
			out.writeUTF(distances);
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            use = ValueUse.valueOf(in.readUTF());
            from = in.readLong();
			to = in.readLong();
			names = in.readUTF();
			distances = in.readUTF();
        }
		
		public void addDistance(long distance) {
			this.distances += distance + " ";
		}

		public void addName(long name) {
			this.names += name + " ";
		}

		public ValueUse getType() {
			return use;
		}

		public long getTo() {
			return to;
		}

		public void setDistances(String s) {
			this.distances = s;
		}

		public void setNames(String s) {
			this.names = s;
		}	
    }


   
	
	/* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, LongWritable, EValue> {
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

        /* Will need to modify to not loose any edges. */
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
			EValue edge = new EValue(ValueUse.EDGE, key.get(), value.get());
			context.write(key, edge);
            context.getCounter(ValueUse.EDGE).increment(1);
        }
    }
	
	public static class LoaderReduce extends Reducer<LongWritable, EValue, LongWritable, EValue> {
		
		public long denom;
		
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
		
		public void reduce(LongWritable key, Iterable<EValue> values, Context context)
				throws IOException, InterruptedException {
			EValue data = new EValue(ValueUse.DATA, key.get(), 0L);
			if (Math.random() < 1.0 / denom) {
			    EValue search = new EValue(ValueUse.SEARCH, key.get(), key.get());
				search.addDistance(-1);
				search.addName(key.get());				
				context.write(key, data);
				for (EValue e : values) {
					context.write(key, e);
					context.write(new LongWritable(e.getTo()), search);
				}	
			} else {
				context.write(key, data);
				for (EValue e : values) {
					context.write(key, e);
				}
			}	
		}
	}	
			

	public static class BFSMap extends Mapper<LongWritable, EValue, LongWritable, EValue> {
		public void map(LongWritable key, EValue value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class BFSReduce extends Reducer<LongWritable, EValue, LongWritable, EValue> {
		public void reduce(LongWritable key, Iterable<EValue> values, Context context)
				throws IOException, InterruptedException {
		    boolean hasChanged = false;
			ArrayList<EValue> stored = new ArrayList<EValue>();
			EValue data = new EValue(ValueUse.DATA, -1, -1);
			for (EValue e : values) {
				if (e.getType() == ValueUse.DATA) {
				    data = new EValue(e);
				} else {
					EValue temp = new EValue(e);
					stored.add(temp);
				}
			}
			ArrayList<EValue> nonSearch = new ArrayList<EValue>();
			for (int i = 0; i < stored.size(); i += 1) {
				EValue e = stored.get(i);
				if (e.getType() == ValueUse.SEARCH) {
					String names = e.names;
					String distances = e.distances;
					Pattern p = Pattern.compile("[-]?[\\d]+ ");
					Matcher m1 = p.matcher(names);
					Matcher m2 = p.matcher(distances);
					while (m1.find() && m2.find()) {
						if (!data.names.contains(m1.group(0))) {
							data.names += m1.group(0);
							long d = Long.parseLong(m2.group(0).trim()) + 1;
							data.addDistance(d);
							hasChanged = true;
						}
					}
				} else {
					EValue temp = new EValue(e);
					nonSearch.add(temp);
				}	
			}
			EValue sentSearch = new EValue(ValueUse.SEARCH, data.distances, data.names);
			for (EValue e : nonSearch) {
				context.write(new LongWritable(e.getTo()), sentSearch);
				context.write(key, e);
			}
			if (hasChanged) {	
			    context.getCounter(ValueUse.DATA).increment(1);	
			}								    
			context.write(key, data);
		}
	}	
		
    
	public static class CleanupMap extends Mapper<LongWritable, EValue, LongWritable, LongWritable> {
		public void map(LongWritable key, EValue value, Context context)
				throws IOException, InterruptedException {
			LongWritable ONE = new LongWritable(1L);
			if (value.getType() == ValueUse.DATA) {
				String d = value.distances;
				Pattern p = Pattern.compile("[\\d]+ ");
				Matcher m = p.matcher(d);
				while (m.find()) {
					long distance = Long.parseLong(m.group(0).trim());
					context.write(new LongWritable(distance), ONE);
				}
			}
		}
	}

	public static class CleanupReduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable v : values) {
				sum += 1;
			}
			context.write(key, new LongWritable(sum));
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
        job.setMapOutputValueClass(EValue.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(EValue.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

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
        while (i<MAX_ITERATIONS) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(EValue.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(EValue.class);

            job.setMapperClass(BFSMap.class);
            job.setReducerClass(BFSReduce.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));
            job.waitForCompletion(true);
	    if (job.getCounters().findCounter(ValueUse.DATA).getValue() == 0) {
	    	i++;
	    	break;
	    }
	    i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(CleanupMap.class);
        //job.setCombinerClass(Reducer.class);
        job.setReducerClass(CleanupReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}