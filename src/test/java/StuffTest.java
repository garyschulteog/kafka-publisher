import com.amazonaws.services.s3.AmazonS3URI;
import org.junit.Assert;
import org.junit.Test;

public class StuffTest {


  @Test
  public void testURIParse() {
    AmazonS3URI uri = new AmazonS3URI("s3://ogsandbox-di-kafka-publisher/general/mytestmessage/message.txt");
    String topic = uri.getKey().split("/")[0];
    Assert.assertEquals("general", topic);
  }

  @Test
  public void testFullURIParse() {
    String fullPath = "s3://ogsandbox-di-kafka-publisher/topic=general/entity_id=GaryTestEntity/schedule_id=1/message.txt";
    POCProducer.URIElements uri = new POCProducer.URIElements(fullPath);


    Assert.assertEquals(fullPath, uri.fullPath);
    Assert.assertEquals("general", uri.topic);
    Assert.assertEquals("ogsandbox-di-kafka-publisher", uri.bucket);
    Assert.assertEquals("general", uri.topic);
    Assert.assertEquals("GaryTestEntity", uri.entityId);
    Assert.assertEquals("1", uri.scheduleId);
    Assert.assertEquals("message.txt", uri.resource);
  }

}
