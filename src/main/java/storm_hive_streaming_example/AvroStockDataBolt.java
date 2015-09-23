/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm_hive_streaming_example;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import storm_hive_streaming_example.model.Stock;
import storm_hive_streaming_example.serializer.StockAvroSerializer;

/**
 *
 * @author hkropp
 */
public class AvroStockDataBolt extends BaseBasicBolt {

    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
        ofDeclarer.declare(new Fields("day", "open", "high", "low", "close", "volume", "adj_close", "name"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        try {
            Object obj = tuple.getValue(0);
            System.out.println(obj.getClass());
            Stock stock = (Stock) tuple.getValue(0);
            //Date,Open,High,Low,Close,Volume,Adj Close,Name
            Values values = new Values(
                                      stock.getDate(),
                                      stock.getOpen(),
                                      stock.getHigh(),
                                      stock.getLow(),
                                      stock.getClose(),
                                      stock.getVolume(),
                                      stock.getAdjClose(),
                                      stock.getName());
            outputCollector.emit(values);
        } catch (Exception ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
            throw new FailedException(ex.toString());
        }
    }
}
