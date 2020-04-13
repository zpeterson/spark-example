package com.spark.assignment1

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object Assignment1 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }

  /**
    *
    * To find the longest trip, take the max value from trips.duration.
    */
  def problem1(tripData: RDD[Trip]): Long = {
    tripData.map(x => x.duration).max()
  }

  /**
    *
    * To figure out how many trips start at the "San Antonio Shopping Center" station,
    * filter the trips to those where trips.start_station are equal to "San Antonio Shopping Center"
    * and count the number of records.
    */
  def problem2(trips: RDD[Trip]): Long = {
    trips.filter(x => x.start_station.equalsIgnoreCase("San Antonio Shopping Center")).count()
  }

  /**
    *
    * To list all the subscriber types from the trips dataset,
    * return the distinct values from trips.subscriber_type.
    */
  def problem3(trips: RDD[Trip]): Seq[String] = {
    trips.map(x => x.subscriber_type).distinct().collect()
  }

  /**
    *
    * To find the busiest zip code, first count the number of times each zip codes appears in
    * trips.zip_code, which creates an array of (zip_code, count) key-value pairs.
    * Then, find the key-value pair with the highest count (value), and return the zip code (key).
    */
  def problem4(trips: RDD[Trip]): String = {
    trips.map(x => x.zip_code).countByValue().maxBy(y => y._2)._1
  }

  /**
    *
    * To find the number of rides that last overnight,
    * first convert trips.start_date and trips.end_date to from Strings to LocalDateTimes.
    * Then, compare the day of month from each date.
    * If they are equal, then the ride did not last overnight.
    * If they aren't equal, then add that ride to the count of overnight rides.
    */
  def problem5(trips: RDD[Trip]): Long = {
    trips
      .filter(x => !parseTimestamp(x.start_date).getDayOfMonth().equals(parseTimestamp(x.end_date).getDayOfMonth))
      .count()
  }

  /**
    *
    * To find the total number of records in the trips dataset, use org.apache.spark.rdd.RDD#count(),
    * which "[returns] the number of elements in the RDD."
    */
  def problem6(trips: RDD[Trip]): Long = {
    trips.count()
  }

  /**
    *
    * To find the ratio of rides that last overnight,
    * divide the number of overnight trips (calculated in problem 5)
    * by the total number of trips (calculated in problem 6).
    */
  def problem7(trips: RDD[Trip]): Double = {
    problem5(trips).doubleValue() / problem6(trips).doubleValue()
  }

  /**
    *
    * To find the sum of double each trip, get the current sum of trips.duration and multiply by 2.
    */
  def problem8(trips: RDD[Trip]): Double = {
    trips.map(x => x.duration).sum() * 2
  }

  /**
    *
    * To find the starting coordinates of the trip with the id 913401,
    * first find the trip.start_station where trip.id is equal to 913401.
    * Then, find the lat and lon from stations where station.name is equal to trip.start_station.
    */
  def problem9(trips: RDD[Trip], stations: RDD[Station]): (Double, Double) = {
    val start = trips.filter(x => x.trip_id.equals("913401")).map(y => y.start_station).first()
    val lat: Double = stations.filter(x => x.name.equals(start)).map(y => y.lat).first()
    val lon: Double = stations.filter(x => x.name.equals(start)).map(y => y.lon).first()
    (lat, lon)
  }

  /**
    *
    * To sum the duration of all trips by starting station,
    * create an array of station names from stations dataset (station_names).
    * Then, loop through the array, and
    * 1. filter the trips to those where the trips.start_station equals the given station name (trip_by_name);
    * 2. sum the duration of those trips (sum_duration);
    * 3. if the sum is greater than zero, append the name and sum_duration to an ArrayBuffer (station_duration).
    * Finally, return station_duration as an Array.
    */
  def problem10(trips: RDD[Trip], stations: RDD[Station]): Array[(String, Long)] = {
    val station_duration = ArrayBuffer[(String, Long)]()
    val station_names = stations.map(x => x.name).collect()
    for (name <- station_names) {
      val trip_by_name = trips.filter(y => y.start_station.equalsIgnoreCase(name))
      val sum_duration = trip_by_name.map(z => z.duration).sum().asInstanceOf[Long]
      if (sum_duration > 0) {
        station_duration.append((name, sum_duration))
      }
    }
    station_duration.toArray
  }

  /*
   DataFrames
   */

  /**
    *
    * To select the "trip_id" column, use
    * org.apache.spark.sql.Dataset#select(org.apache.spark.sql.TypedColumn, org.apache.spark.sql.TypedColumn).
    */
  def dfProblem11(trips: DataFrame): DataFrame = {
    trips.select(col = "trip_id")
  }

  /**
    *
    * To get all the trips starting at "Harry Bridges Plaza (Ferry Building)" station,
    * filter trips to those where trips.start_station equals "Harry Bridges Plaza (Ferry Building)."
    */
  def dfProblem12(trips: DataFrame): DataFrame = {
    trips.filter(trips("start_station") === "Harry Bridges Plaza (Ferry Building)")
  }

  /**
    *
    * To sum the duration of all trips, create an aggregate of the sum of trips.duration,
    * and extract the first element from the aggregate as a Long.
    */
  def dfProblem13(trips: DataFrame): Long = {
    trips.agg(sum("duration")).first().get(0).asInstanceOf[Long]
  }

  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))

}
