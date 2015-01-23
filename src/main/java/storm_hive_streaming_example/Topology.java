/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm_hive_streaming_example;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 *
 * @author hkropp
 */
public class Topology {

    public static final String KAFKA_SPOUT_ID = "kafka-spout";
    public static final String STOCK_PROCESS_BOLT_ID = "stock-process-bolt";
    public static final String HIVE_BOLT_ID = "hive-stock-price-bolt";

    public static void main(String... args) {

        String kafkaTopic = "stock_topic";

        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("127.0.0.1"),
                kafkaTopic, "/kafka_storm", "StormSpout");
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
        spoutConfig.startOffsetTime = System.currentTimeMillis();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        
        // Hive connection configuration
        String metaStoreURI = "thrift://sandbox.hortonworks.com:9083";
        String dbName = "default";
        String tblName = "stock_prices";
        // Fields for possible partition
        String[] partNames = {"name"};
        // Fields for possible column data
        String[] colNames = {"day", "open", "high", "low", "close", "volume","adj_close"};
        // Record Writer configuration
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames))
                .withPartitionFields(new Fields(partNames));

        HiveOptions hiveOptions;
        hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(100)
                .withIdleTimeout(10);
                //.withKerberosKeytab(path_to_keytab)
                //.withKerberosPrincipal(krb_principal);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
        builder.setBolt(STOCK_PROCESS_BOLT_ID, new StockDataBolt()).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(HIVE_BOLT_ID, new HiveBolt(hiveOptions)).shuffleGrouping(STOCK_PROCESS_BOLT_ID);
        
        String topologyName = "StormHiveStreamingTopo";
        Config config = new Config();
        config.setNumWorkers(1);
        config.setMessageTimeoutSecs(60);
        try {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static class StockDataBolt extends BaseBasicBolt {
        
        private DateFormat df = new SimpleDateFormat ("yyyy-MM-dd");
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
            ofDeclarer.declare(new Fields("day","open","high","low","close","volume","adj_close","name"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
            Fields fields = tuple.getFields();
            try {
                String stockDataStr = new String((byte[]) tuple.getValueByField(fields.get(0)), "UTF-8");
                String[] stockData = stockDataStr.split(",");
                Values values = new Values(df.parse(stockData[0]), Float.parseFloat(stockData[1]),
                        Float.parseFloat(stockData[2]),Float.parseFloat(stockData[3]),
                        Float.parseFloat(stockData[4]),Integer.parseInt(stockData[5]), 
                        Float.parseFloat(stockData[6]),stockData[7]);
                outputCollector.emit(values);
            } catch (UnsupportedEncodingException | ParseException ex) {
                Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
                throw new FailedException(ex.toString());
            }      
        }
    }
}
