package com.example.elastic.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@IdClass(MyKey.class)
@Table(name = "user_activity", schema = "public")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserActivityDB {
	@Id
	@Column(name= "user_id")
	private String user_id;

	@Id
	@Column(name = "url")
	private String url;

	@Column(name = "count")
	private int count;
	@Id
	@Column(name="date")
	private String time;

	@Column(name = "total_time")
	private float total_time;
}
