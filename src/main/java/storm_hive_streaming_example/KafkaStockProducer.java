/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm_hive_streaming_example;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author hkropp
 */
public class KafkaStockProducer {

    public static void main(String... args) throws IOException, InterruptedException {
        // Kafka Properties
        Properties props = new Properties();
        // HDP uses 6667 as the broker port. Sometimes the binding is not resolved as expected, therefor this list.
        props.put("metadata.broker.list", "10.0.2.15:6667,127.0.0.1:6667,sandbox.hortonworks.com:6667");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        
        // Topic Name
        String topic = "stock_topic";
        
        // Create Kafka Producer
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);
        
        // Read Data from File Line by Line
        List<String> stockPrices = IOUtils.readLines(
                KafkaStockProducer.class.getResourceAsStream("stock.csv"),
                Charset.forName("UTF-8")
        );
        // Send Each Line to Kafka Producer and Sleep
        for(String line : stockPrices){
            System.out.println(line);
            if(line.startsWith("Date")) continue;
            producer.send(new KeyedMessage<String, String>(topic, line));
            Thread.sleep(50L);
        }
        producer.close();
    }

}
