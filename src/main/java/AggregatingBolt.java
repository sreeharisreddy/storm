import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

public class AggregatingBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Field"));
    }


    public void execute(TupleWindow tupleWindow) {
        List<Tuple> touples = tupleWindow.get();

        int sum = 0;
        for (Tuple t:touples) {
            sum = sum + t.getInteger(0);
        }
        outputCollector.emit(new Values(sum));
    }
}
