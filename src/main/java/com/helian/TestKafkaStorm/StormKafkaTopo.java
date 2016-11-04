package com.helian.TestKafkaStorm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class StormKafkaTopo {   
    public static void main(String[] args) throws Exception { 
        // 配置Zookeeper地址
        BrokerHosts brokerHosts = new ZkHosts("192.168.44.135:2181,192.168.44.129:2181,192.168.44.137:2181");//,192.168.44.129:2181
        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "test2", "/kafka2" , "kafkaspout");
        
       // 配置KafkaBolt中的kafka.broker.properties
        Config conf = new Config();  
        Map<String, String> map = new HashMap<String, String>(); 
       // 配置Kafka broker地址       
        map.put("metadata.broker.list", "192.168.44.135:9092,192.168.44.129:9092,192.168.44.137:9092");
        // serializer.class为消息的序列化类
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        // 配置KafkaBolt生成的topic
        conf.put("topic", "topic2");
        conf.setDebug(true);
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());  
        TopologyBuilder builder = new TopologyBuilder();   
        builder.setSpout("spout", new KafkaSpout(spoutConfig));  
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout"); 
        builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt");        

        if (args != null && args.length > 0) {  
        	//集群
            conf.setNumWorkers(3);  
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        } else {  
        	//本地
            LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("Topo", conf, builder.createTopology());  
            Utils.sleep(100000);  
            cluster.killTopology("Topo");  
            cluster.shutdown();  
        }  
    }  
    	/*String zks = "192.168.44.135:2181,192.168.44.129:2181,192.168.44.137:2181";
		String topic = "test2";
		String zkRoot = "/kafka2";
		String id = "kafkaspout";
		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.zkServers = Arrays.asList(new String[] { "192.168.44.135", "192.168.44.129","192.168.44.137"});
		spoutConf.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConf), 1);
		builder.setBolt("bolt", new SenqueceBolt(), 1).shuffleGrouping("spout");
		
		Config conf = new Config();
		String name = StormKafkaTopo.class.getSimpleName();
		if (args != null && args.length > 0) {
			name = args[0];
			conf.put(Config.NIMBUS_HOST, "192.168.44.135");
			conf.setNumWorkers(4);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(4);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}

	}*/
}