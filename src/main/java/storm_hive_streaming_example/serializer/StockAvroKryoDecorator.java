package storm_hive_streaming_example.serializer;

import backtype.storm.serialization.IKryoDecorator;
import com.esotericsoftware.kryo.Kryo;
import storm_hive_streaming_example.model.Stock;

public class StockAvroKryoDecorator implements IKryoDecorator {

    public void decorate(Kryo k) {
        k.register(Stock.class, new StockAvroSerializer());
    }

}
