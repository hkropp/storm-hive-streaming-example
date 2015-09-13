/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm_hive_streaming_example;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
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

/**
 *
 * @author hkropp
 */
public class StockDataBolt extends BaseBasicBolt {

    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
        ofDeclarer.declare(new Fields("day", "open", "high", "low", "close", "volume", "adj_close", "name"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        Fields fields = tuple.getFields();
        try {
            String stockDataStr = new String((byte[]) tuple.getValueByField(fields.get(0)), "UTF-8");
            String[] stockData = stockDataStr.split(",");
            Values values = new Values(df.parse(stockData[0]), Float.parseFloat(stockData[1]),
                    Float.parseFloat(stockData[2]), Float.parseFloat(stockData[3]),
                    Float.parseFloat(stockData[4]), Integer.parseInt(stockData[5]),
                    Float.parseFloat(stockData[6]), stockData[7]);
            outputCollector.emit(values);
        } catch (UnsupportedEncodingException | ParseException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
            throw new FailedException(ex.toString());
        }
    }
}
