package com.example.elastic.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName="network_packet",shards = 2)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserActivity {

    @Id
    private String id;
    @Field(name="url")
    //@JsonProperty("message")
    private String url;
    @Field(name="localdate")
    //@JsonProperty("@timestamp")
    //@Field(type = FieldType.Date, format = DateFormat.custom, pattern = "uuuu-MM-dd'T'HH:mm:ss.SSSZZ")
    private String time;
    @Field(name="user_id")
    private String user_id;

}
