package com.example.elastic.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Field;

@Document(indexName="network_packet")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserActivity {

    @Id
    private String id;
    @Field(name="message")
    //@JsonProperty("message")
    private String url;
    @Field(name="@timestamp")
    //@JsonProperty("@timestamp")
    private String time;
    @Field(name="user_name")
    private String user_id;

}
