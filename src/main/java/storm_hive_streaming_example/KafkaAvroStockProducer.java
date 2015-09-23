package storm_hive_streaming_example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import storm_hive_streaming_example.model.Stock;

/**
 * Created by hkropp on 23/09/15.
 */
public class KafkaAvroStockProducer {
    public static void main(String... args) throws IOException, InterruptedException {
        // Kafka Properties
        Properties props = new Properties();
        // HDP uses 6667 as the broker port. Sometimes the binding is not resolved as expected, therefor this list.
        props.put("metadata.broker.list", "127.0.0.1:6667,one.hdp:6667");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "1");

        // Topic Name
        String topic = "stock_topic";

        // Create Kafka Producer
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, byte[]> producer = new Producer<>(config);

        // Read Data from File Line by Line
        List<String> stockPrices = IOUtils.readLines(
                                                    KafkaStockProducer.class.getResourceAsStream("stock.csv"),
                                                    Charset.forName("UTF-8")
        );
        // Send Each Line to Kafka Producer and Sleep
        for(String line : stockPrices){
            System.out.println(line);
            if(line.startsWith("Date")) continue;
            String[] stockData = line.split(",");
            // Date,Open,High,Low,Close,Volume,Adj Close,Name
            Stock stock = new Stock();
            stock.setDate(stockData[0]);
            stock.setOpen(Float.parseFloat(stockData[1]));
            stock.setHigh(Float.parseFloat(stockData[2]));
            stock.setLow(Float.parseFloat(stockData[3]));
            stock.setClose(Float.parseFloat(stockData[4]));
            stock.setVolume(Integer.parseInt(stockData[5]));
            stock.setAdjClose(Float.parseFloat(stockData[6]));
            stock.setName(stockData[7]);
            KeyedMessage<String, byte[]> data =
                        new KeyedMessage<String, byte[]>(topic, serialize(stock));
            producer.send(data);
            Thread.sleep(50L);
        }
        producer.close();
    }

    public static <T extends SpecificRecordBase> byte[] serialize(T record) throws IOException {
        try {
            DatumWriter<T> writer = new SpecificDatumWriter<T>(record.getSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            IOUtils.closeQuietly(out);
            return out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
