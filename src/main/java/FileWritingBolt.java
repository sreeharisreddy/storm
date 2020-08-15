import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;

public class FileWritingBolt extends BaseRichBolt {
    private BufferedWriter writer;
    private String filePath;
    public FileWritingBolt(String filePath){
        this.filePath = filePath;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            writer = new BufferedWriter(new FileWriter(filePath));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
       int sum = tuple.getInteger(0);
try {


        writer.write(""+sum);
        writer.newLine();
        writer.flush();
}catch (Exception e){
    e.printStackTrace();
}
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
