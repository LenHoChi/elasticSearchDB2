package com.example.elastic;

import com.easyquartz.scheduler.ScheduleService;
import com.example.elastic.model.UserActivityDB;
import com.example.elastic.model.Users;
import com.example.elastic.repository.UsersRepository;
import com.example.elastic.service.UserActService;

import org.jboss.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static org.hibernate.tool.schema.SchemaToolingLogging.LOGGER;

@SpringBootApplication
@ComponentScan({"com.example","com.easyquartz"})
public class ElasticApplication {
//	private static ScheduleService scheduleService;
//	@Autowired
//	public ElasticApplication(ScheduleService scheduleService){
//		ElasticApplication.scheduleService = scheduleService;
//	}
//	public static void startCron(){
//		scheduleService.schedule(UserActService.class, "lenho15","0 22 11,16 ? * *");
//				//"","0 50 11,13 ? * *");
//			//	"","0/15 * * * * ? *");
//	}
	public static void main(String[] args) {
		SpringApplication.run(ElasticApplication.class, args);
//		startCron();
//		int i=6,j=1;
////		System.out.println("i1--->"+i++);
////		System.out.println("i2------->"+i);
////		System.out.println(++j);
//		int x = i*=4;
//		System.out.println(x);
//		System.out.println("CONNECT gjghjhh".matches("CONNECT+\\s"));
/*		System.out.println(checkContain("CONNECT "));
		System.out.println(checkContain("CONNECT  "));
		System.out.println(checkContain("CONNECT dfdfddf"));
		System.out.println(checkContain("CONNET dffdf connect"));
		System.out.println(checkContain("CONNECT fd.com:443 HTTP1/1"));*/
//		LOGGER.log(Logger.Level.INFO,"Total execution time2: ");
//		LOGGER.log(Logger.Level.DEBUG,"Total execution time2: ");
//		LOGGER.log(Logger.Level.DEBUG,"Total execution time2: ");
//
//		LOGGER.info("dfdffd");
		//String[] arr = new String[10];
//		String[] arr = {"1","2","3","4","5"};
//		for(int i=0;i<arr.length;i++){
//			if(arr[i]=="3")
//				continue;
//			System.out.println(arr[i]);
//		}
//		List<String> list = new ArrayList<String>();
//		list.add("Java");
//		list.add("PHP");
//		list.add("C++");
//		list.add("Python");
//		for(int i=0;i< arr.length;i++) {
//			list.forEach((ele) -> {
//				if (ele == "C++")
//					return;
//				System.out.println(ele);
//			});
//			System.out.println(arr[i]);
//		}
//		final long startTime = System.currentTimeMillis();
//		for(int i=0;i<100000;i++){
//			if(i%2==0)
//				System.out.println(i);
//		}
//		final long endTime = System.currentTimeMillis();
//		LOGGER.log(Logger.Level.INFO, "Total execution time: " + (endTime - startTime));
	}
}
