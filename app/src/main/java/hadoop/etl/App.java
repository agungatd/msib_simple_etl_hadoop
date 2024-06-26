/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package hadoop.etl;

public class App {
    public String startAppMsg() {
        return "Hadoop ETL Process start!";
    }
    public String endAppMsg() {
        return "Hadoop ETL Process finish!";
    }

    public static void main(String[] args) {
        System.out.println(new App().startAppMsg());

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ETL Job");
        job.setJarByClass(ETLDriver.class);

        job.setMapperClass(ETLMapper.class);
        job.setReducerClass(ETLReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/home/agung_msbu/personal/digitalSkola/simple-java/hadoop-etl/app/src/main/resources/data/data.txt")); // Path to your data file
        FileOutputFormat.setOutputPath(job, new Path("/home/agung_msbu/personal/digitalSkola/simple-java/hadoop-etl/app/src/main/resources/data/new_data.txt")); // Path to store output

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        System.out.println(new App().endAppMsg());
    }
}
