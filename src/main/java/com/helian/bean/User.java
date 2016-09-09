package com.helian.bean;

import java.util.Date;

public class User {

	private String name;
	private Integer age;
	private Date time;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getAge() {
		return age;
	}
	public void setAge(Integer age) {
		this.age = age;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	@Override
	public String toString() {
		return "User [name=" + name + ", age=" + age + ", time=" + time + "]";
	}
	
}
