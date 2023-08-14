package Processor;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime.getApplicationProperties;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String INPUT_STREAM_NAME = "";
    private static String S3_SINK_PATH = "";
    private static String AWS_REGION = "us-east-1";
    public static final String AWS_KINESIS_STREAM_NAME = "aws.kinesis.stream.name";
    private static ParameterTool parameters;
    private static final String INPUT_STREAM = "input-stream";
    private static final String S3_SINK = "s3-sink";

    public static Map<String, String> getConsumerProperties() throws IOException {
        Map<String, Properties> applicationProperties = getApplicationProperties();
        Properties properties = applicationProperties.get("ConsumerConfigProperties");
        Map<String, String> params = new HashMap<>();
        properties.forEach( (k,v) -> params.put((String)k, (String)v));
        return params;
    }

    public static ParameterTool getParameters() {
        try {
            Map<String, String> properties = getConsumerProperties();

            // Fetch and calculate properties
            AWS_REGION = properties.getOrDefault("aws.region", "us-east-1");
            S3_SINK_PATH = properties.getOrDefault("s3SinkPath", "");
            if(S3_SINK_PATH.isEmpty()) {
                LOG.error("S3 Sink Path not specified");
                return null;
            }
            properties.put("getRecordsIntervalMs", "60000"); // 60secs
            if (!properties.containsKey(AWS_KINESIS_STREAM_NAME)) {
                LOG.error("aws.kinesis.stream.name not specified");
                return null;
            }
            return ParameterTool.fromMap(properties);
        }
        catch (Exception ex) {
            LOG.error(ex.toString());
        }
        return null;
    }

    /**
     * Creates the AWS Kinesis configuration properties based on the arguments passed to the application.
     * @return Properties
     */
    private static Properties getKinesisProperties() {
        Properties kinesisConsumerConfig = new Properties();
        // Set the region the Kinesis stream is located in
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, AWS_REGION);
        kinesisConsumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        // poll new events from the Kinesis stream once every x seconds
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
                String.valueOf(parameters.get("getRecordsIntervalMs")));
        return kinesisConsumerConfig;
    }
    private static DataStream<String> getSourceStream(StreamExecutionEnvironment env) {
        String kinesisStream = parameters.get(AWS_KINESIS_STREAM_NAME);
        Properties kinesisConsumerConfig = getKinesisProperties();
        // create Kinesis source
        return env.addSource(new FlinkKinesisConsumer<>(
                        // read events from the Kinesis stream passed in as a parameter
                        kinesisStream,
                        // deserialize events with EventSchema
                        new SimpleStringSchema(),
                        // using the previously defined properties
                        kinesisConsumerConfig
                ))
                .name(INPUT_STREAM).uid(INPUT_STREAM);
    }

    private static StreamingFileSink<String> createS3SinkFromStaticConfig() {
        return StreamingFileSink
                .forRowFormat(new Path(S3_SINK_PATH), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        parameters = getParameters();
        DataStream<String> events = getSourceStream(env);
        events.addSink(createS3SinkFromStaticConfig()).uid(S3_SINK).name(S3_SINK);

        env.getConfig().setGlobalJobParameters(parameters);
        env.getConfig().setAutoWatermarkInterval(100);
        env.execute("Event Processor");

    }





}
