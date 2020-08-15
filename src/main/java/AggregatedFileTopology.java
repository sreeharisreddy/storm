import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class AggregatedFileTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        IntegerSpout intsp = new IntegerSpout();
        builder.setSpout("IntegerSpout", new IntegerSpout());
        builder.setBolt("MultiplierBolt", new MultiplierBolt()).shuffleGrouping("IntegerSpout");

        AggregatingBolt aggregationBolt = (AggregatingBolt) new AggregatingBolt()
        //.withTimestampField("timestamp")
        .withLag(BaseWindowedBolt.Duration.seconds(1))
        .withWindow(BaseWindowedBolt.Duration.seconds(10));

        builder.setBolt("aggregatingBolt", aggregationBolt).shuffleGrouping("MultiplierBolt");
        String filePath = "./src/main/resources/data.txt";
        FileWritingBolt file = new FileWritingBolt(filePath);
        builder.setBolt("fileBolt", file)
                .shuffleGrouping("aggregatingBolt");

        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Test", config, builder.createTopology());
            Thread.sleep(20000);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
