package com.my.org.fast_users

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkApp extends App {
  Runner.run(new SparkConf().setAppName("User response speed Vatolin"))
}

object Runner {
  def run(conf: SparkConf): Unit = {
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val posts = spark.table("posts")
      .withColumn("creationdate", unix_timestamp('creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS").cast(TimestampType))
    val users = spark.table("users").select('id.alias("user_id"), 'displayname, 'reputation)

    val questions = posts.where('posttypeid === 1)
      .select('id.alias("question_id"), 'AcceptedAnswerId, 'creationdate.alias("question_creationdate"))
    val answers = posts.where('posttypeid === 2)
      .select('id.alias("answer_id"), 'creationdate.alias("answer_creationdate"), 'owneruserid)

    val answer_question = questions
        .join(answers, questions.col("AcceptedAnswerId") === answers.col("answer_id"), "inner")
        .withColumn("time_delta",
          ('answer_creationdate.cast(LongType) - 'question_creationdate.cast(LongType)) / 60)
        .where('time_delta > 0.3)
        .groupBy('owneruserid).agg(mean("time_delta").alias("mean_time_delta"),
        count("time_delta").alias("answers_count"))
        .where('answers_count > 1)

    val result = answer_question.join(users, users.col("user_id") === answer_question.col("owneruserid"))
        .orderBy('time_delta)
    result.show(20)
    result.write.saveAsTable("vatolin_scala_response_speed")
  }
}
