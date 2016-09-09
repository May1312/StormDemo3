package com.helian.TestKafkaStorm;

import java.io.UnsupportedEncodingException;
import java.util.List;
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageScheme implements Scheme {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2719003841396631027L;


	public List<Object> deserialize(byte[] ser) {
        try {
            String msg = new String(ser, "UTF-8"); 
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {  
         
        }
        return null;
    }
    
    
    public Fields getOutputFields() {
        // TODO Auto-generated method stub
        return new Fields("msg");  
    }

}
