package com.example.elastic.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Field;

@Document(indexName="test3",shards=2)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserActivity {

    @Id
    private String id;
    @Field(name="url")
    //@JsonProperty("message")
    private String url;
    @Field(name="@timestamp")
    //@JsonProperty("@timestamp")
    private String time;
    @Field(name="user_id")
    private String user_id;

}
