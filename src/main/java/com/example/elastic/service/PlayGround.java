package com.example.elastic.service;

import com.easyquartz.scheduler.ScheduleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PlayGround {
    private ScheduleService scheduleService;

    @Autowired
    public PlayGround(ScheduleService scheduleService){
        this.scheduleService = scheduleService;
    }

    public void startCron(){
        scheduleService.schedule(UserActService.class, " 0/15 * * * * ? *");
    }

}
