import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class POCProducer {


  //TODO: this can/should be discovered via kubernetes service bridge in vertx land
  private final static String BOOTSTRAP_SERVERS =
      "100.68.65.93:9092";


    private static Producer<Long, String> createProducer() {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          BOOTSTRAP_SERVERS);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "POCProducer");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          LongSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          StringSerializer.class.getName());
      return new KafkaProducer<>(props);
    }


  /**
   * read topic and message from s3 path that should be an argument to the docker container, of the format:
   *
   * s3://[bucket_name]/[topic_name]/[schedule_name]/[message_file]
   */
  public static void main(String args[]) {
      try(Producer producer = createProducer()) {

        String s3Path = args[0];

        Long time = System.currentTimeMillis();
        ProducerRecord <Long, String> z = buildRecord(s3Path, time);
        Future<RecordMetadata> fut = producer.send(z);
        RecordMetadata metadata = fut.get();

        System.out.printf("sent record (key= %s, value = %s) meta(partition=%d, offset = %d) time - %d",
            z.key(), z.value(), metadata.partition(), metadata.offset(), (System.currentTimeMillis() - time));

        producer.flush();
      } catch(Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }

    private static ProducerRecord buildRecord(String s3Path, Long key) {
      // cheap topic parse regex, super unsafe
      URIElements uri = new URIElements(s3Path);
      String body = getMyMessage(uri);
      return new ProducerRecord<>(uri.topic, key, body);
    }

    //ugly for poc
    private static String getMyMessage(URIElements uri) {
      String body ="";
      try {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object s3obj = s3.getObject(new GetObjectRequest(uri.bucket, uri.key));
        body = IOUtils.toString(s3obj.getObjectContent());
        body = body.replace("${date}", new Date().toString())
            .replace("${topic}", uri.topic)
            .replace("${entityId}", uri.entityId);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return body;
    }

  /* hi I am dirty, clean me */
  static class URIElements {
      final String fullPath, key, bucket, topic, entityId, scheduleId, resource;
      public URIElements(String path) {
        fullPath = path;
        AmazonS3URI uri = new AmazonS3URI(path);
        bucket = uri.getBucket();
        key = uri.getKey();
        String elements[] = key.split("/");
        //todo: better validation
        topic = elements[0].replace("topic=","");
        entityId = elements[1].replace("entity_id=","");
        scheduleId = elements[2].replace("schedule_id=","");
        resource = elements[3];
      }
    }
}
