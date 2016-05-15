package com.eversec.xhz;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class datetime {
	//@Test
	public void dateTest(){
		String st = "/tmp/20151124/00/ever50_attac";
		String s = st.substring(5, 16).replace("/", "");
		System.out.println(s);
	}
	
}
