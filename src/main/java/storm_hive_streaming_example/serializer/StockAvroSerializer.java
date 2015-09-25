package storm_hive_streaming_example.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import storm_hive_streaming_example.model.Stock;

public class StockAvroSerializer extends Serializer<Stock> {

    private static final Logger LOG = LoggerFactory.getLogger(StockAvroSerializer.class);
    private Schema SCHEMA = Stock.getClassSchema();

    public void write(Kryo kryo, Output output, Stock object) {
        DatumWriter<Stock> writer = new SpecificDatumWriter<>(SCHEMA);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write(object, encoder);
            encoder.flush();
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        }
        IOUtils.closeQuietly(out);
        byte[] outBytes = out.toByteArray();
        output.writeInt(outBytes.length, true);
        output.write(outBytes);
    }

    public Stock read(Kryo kryo, Input input, Class<Stock> type) {
        byte[] value = input.getBuffer();
        SpecificDatumReader<Stock> reader = new SpecificDatumReader<>(SCHEMA);
        Stock record = null;
        try {
            record = reader.read(null, DecoderFactory.get().binaryDecoder(value, null));
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        }
        return record;
    }
}
