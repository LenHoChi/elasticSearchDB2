package com.example.elastic.convert;

import com.example.elastic.model.UserActivityDB;
import com.example.elastic.model.UserActivity;

public class ToDB {
    public UserActivityDB convertInfoToDB(UserActivity info){
        UserActivityDB dbInfo = new UserActivityDB();
        dbInfo.setUser_id(info.getUser_id());
        dbInfo.setUrl(info.getUrl());
        dbInfo.setTime(info.getTime());
        dbInfo.setTime("2012/2/2");
        dbInfo.setTotal_time(30);
        return dbInfo;
    }
}
