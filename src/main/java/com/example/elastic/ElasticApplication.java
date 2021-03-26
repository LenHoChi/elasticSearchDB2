package com.example.elastic;

import com.easyquartz.scheduler.ScheduleService;
import com.example.elastic.service.UserActService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
@SpringBootApplication
@ComponentScan({"com.example","com.easyquartz"})
public class ElasticApplication {
	private static ScheduleService scheduleService;
	@Autowired
	public ElasticApplication(ScheduleService scheduleService){
		ElasticApplication.scheduleService = scheduleService;
	}
	public static void startCron(){
		scheduleService.schedule(UserActService.class, "lenhochi154" +
				"","0 50 11,13 ? * *");
	}
	public static void main(String[] args) {
		SpringApplication.run(ElasticApplication.class, args);
		startCron();
	}
}
