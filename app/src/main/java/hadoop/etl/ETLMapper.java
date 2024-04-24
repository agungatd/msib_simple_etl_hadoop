public class ETLMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        context.write(new Text(data[0]), new Text(String.join(",", Arrays.copyOfRange(data, 1, data.length))));
    }
}
