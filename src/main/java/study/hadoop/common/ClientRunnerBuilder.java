package study.hadoop.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class ClientRunnerBuilder {

    private Client client;

    public ClientRunnerBuilder(Class<?> jobClass, String jobName) throws IOException {
        client = new Client(jobClass, jobName);
    }

    public ClientRunnerBuilder withMapperClass(Class<? extends Mapper> mapperClass) {
        client.setMapperClass(mapperClass);
        return this;
    }

    public ClientRunnerBuilder withReducerClass(Class<? extends Reducer> reducerClass) {
        client.setReducerClass(reducerClass);
        return this;
    }

    public ClientRunnerBuilder withOutputKeyClass(Class<?> outputKeyClass) {
        client.setOutputKeyClass(outputKeyClass);
        return this;
    }

    public ClientRunnerBuilder withOutputValueClass(Class<?> outputValueClass) {
        client.setOutputValueClass(outputValueClass);
        return this;
    }

    public ClientRunnerBuilder withKeepOutputFolder(boolean keepOutputFolder) {
        client.setKeepOutputFolder(keepOutputFolder);
        return this;
    }

    public Client build() {
        return client;
    }

    public static class Client {
        private Configuration conf;
        private Job job;
        private Class<?> jobClass;
        private boolean keepOutputFolder = true;

        private Client(Class<?> jobClass, String jobName) throws IOException {
            conf = new Configuration();
            conf.set("dfs.client.use.datanode.hostname", "true");
            conf.set("dfs.datanode.use.datanode.hostname", "true");
            job = Job.getInstance(conf, jobName);
            this.jobClass = jobClass;
        }

        void setMapperClass(Class<? extends Mapper> mapperClass) {
            job.setMapperClass(mapperClass);
        }

        void setReducerClass(Class<? extends Reducer> reducerClass) {
            job.setReducerClass(reducerClass);
        }

        void setOutputKeyClass(Class<?> keyClass) {
            job.setOutputKeyClass(keyClass);
        }

        void setOutputValueClass(Class<?> valueClass) {
            job.setOutputValueClass(valueClass);
        }

        void setKeepOutputFolder(boolean keepOutputFolder) {
            this.keepOutputFolder = keepOutputFolder;
        }

        public void run(String resFolder) throws IOException, ClassNotFoundException, InterruptedException {
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            String localInputFolder = resFolder + "/input";
            URL localInputUrl = loader.getResource(localInputFolder);

            FileInputFormat.addInputPath(job, new Path(localInputUrl.getPath()));

            String outputPath = "hdfs://quickstart.cloudera/user/cloudera/" + resFolder + "/output";
            Path hadoopOutputPath = new Path(outputPath);
            FileOutputFormat.setOutputPath(job, hadoopOutputPath);

            job.setJarByClass(jobClass);
            job.waitForCompletion(true);

            if (!keepOutputFolder) {
                FileSystem fs = FileSystem.get(URI.create(outputPath), conf);
                RemoteIterator<LocatedFileStatus> filesStatus = fs.listFiles(hadoopOutputPath, false);
                while (filesStatus.hasNext()) {
                    LocatedFileStatus fileStatus = filesStatus.next();
                    Path filePath = fileStatus.getPath();
                    try (InputStream in = fs.open(filePath)) {
                        IOUtils.copyBytes(in, System.out, 4098);
                    }
                }

                fs.delete(hadoopOutputPath, true);
            }
        }
    }
}
