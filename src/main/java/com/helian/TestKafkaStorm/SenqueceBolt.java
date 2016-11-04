package com.helian.TestKafkaStorm;

import java.util.HashMap;
import java.util.Map;
import com.alibaba.fastjson.JSON;
import com.helian.api.HttpClientUtils;
import com.helian.bean.User;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SenqueceBolt extends BaseBasicBolt{
    
    /**
	 * 
	 */
	private static final long serialVersionUID = -2667199403582786764L;
	
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		
         String word = (String) input.getValue(0);  
         //json字符串转json对象
         User user = JSON.parseObject(word, User.class);
         //讲数据保存到数据库中
        // springService.insertUser(user);
         //url
         String url = "http://192.168.44.135:7777/TestAnnotation/hang/user";
         Map<String,Object> map = new HashMap<String,Object>();
         map.put("user", word);
         try {
			HttpClientUtils.simplePostInvoke2(url,map);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         
         System.out.println(user.toString());
         String out = "See you " + word +  "!";  
         System.out.println("out=" + out);
         collector.emit(new Values(out));
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}
