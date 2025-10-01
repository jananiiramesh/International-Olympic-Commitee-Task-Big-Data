#!/usr/bin/env python3
import sys
import json
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

spark = SparkSession.builder\
                    .master("local")\
                    .appName("Athletic_data")\
                    .getOrCreate()

athletes_2012_df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
athletes_2016_df = spark.read.csv(sys.argv[2], header=True, inferSchema=True)
athletes_2020_df = spark.read.csv(sys.argv[3], header=True, inferSchema=True)
coaches_df = spark.read.csv(sys.argv[4], header=True, inferSchema=True)
medals_df = spark.read.csv(sys.argv[5], header=True, inferSchema=True)

athletes_2012_df = athletes_2012_df.withColumn('y',F.lit(2012))
athletes_2016_df = athletes_2016_df.withColumn('y',F.lit(2016))
athletes_2020_df = athletes_2020_df.withColumn('y',F.lit(2020))

#all years data combined into one datafram
combined_all_years_athletes = athletes_2012_df.union(athletes_2016_df).union(athletes_2020_df)
#filter medals_df to only include years 2012, 2016 and 2020
medals_df = medals_df.filter(medals_df.year.isin([2012,2016,2020]))

#task 1.1
#selecting only relevant fields
req_df_medals = medals_df.select("id", "sport", "year", "medal")

#adding additional column with points for that particular medal and year based on given info
medal_and_points_df = req_df_medals.withColumn("points",
    F.when(req_df_medals.year == 2012, F.when(req_df_medals.medal == "gold", 20)
        .when(req_df_medals.medal == "silver", 15)
        .when(req_df_medals.medal == "bronze", 10))
    .when(req_df_medals.year == 2016, F.when(req_df_medals.medal == "gold", 12)
        .when(req_df_medals.medal == "silver", 8)
        .when(req_df_medals.medal == "bronze", 6))
    .when(req_df_medals.year == 2020, F.when(req_df_medals.medal == "gold", 15)
        .when(req_df_medals.medal == "silver", 12)
        .when(req_df_medals.medal == "bronze", 7))
    .otherwise(0))
##medal_and_points_df.show()

##additional column to track number of gold, silver and bronze medals for that particular athlete
medal_and_points_df = medal_and_points_df.withColumn("gold_medals", F.when(req_df_medals.medal == "gold", 1).otherwise(0))\
    .withColumn("silver_medals", F.when(req_df_medals.medal == "silver", 1).otherwise(0))\
    .withColumn("bronze_medals", F.when(req_df_medals.medal == "bronze", 1).otherwise(0))

#summing up gold, silver and bronze
total_points_for_athletes_df = medal_and_points_df.groupBy("id", "sport") \
    .agg(F.sum("points").alias("total_points_athlete"),
         F.sum("gold_medals").alias("total_gold_medals_athlete"),
         F.sum("silver_medals").alias("total_silver_medals_athlete"),
         F.sum("bronze_medals").alias("total_bronze_medals_athlete"))

total_points_for_athletes_df = total_points_for_athletes_df.orderBy("id", ascending=True)

#finding the max score for each sport using max 
max_points_per_sport_df = total_points_for_athletes_df.groupBy("sport") \
    .agg(F.max("total_points_athlete").alias("highest_points_sport"))

#join to include athlete names, rename to avoid ambiguity
athletes_with_max_points_df = total_points_for_athletes_df.alias("tpa") \
    .join(max_points_per_sport_df.alias("mps"), on="sport")

#filter out max scorers and select required columns
max_scorers_df = athletes_with_max_points_df.filter(F.col("tpa.total_points_athlete") == F.col("mps.highest_points_sport")) \
    .join(combined_all_years_athletes.select("id", "name", "sport").alias("caya"), "id")  #alias again :)

#window for proper ranking
window = Window.partitionBy("tpa.sport") \
    .orderBy(F.col("tpa.total_gold_medals_athlete").desc(),
              F.col("tpa.total_silver_medals_athlete").desc(),
              F.col("tpa.total_bronze_medals_athlete").desc(),
              F.col("caya.name").asc())  #if number of gold, silver and bronze is same, take name as deciding factor

ranked_scorers_df = max_scorers_df.withColumn("rank", F.row_number().over(window))

final_scorers_df = ranked_scorers_df.filter(F.col("rank") == 1) ##to get the top performer

#select distinct athlete names
required_athlete_names_df = final_scorers_df.select("caya.id", "caya.name", "tpa.sport").distinct()
required_athlete_names_df = required_athlete_names_df.orderBy("tpa.sport")

#collect results we need :)
list_of_top_performers = [row.name.upper() for row in required_athlete_names_df.collect()]

##task 1.2

combined_all_years_athletes_lower = combined_all_years_athletes.withColumn("country",F.lower(F.col("country")))
medals_df = medals_df.withColumn("medal",F.lower(F.col("medal")))

china_india_usa = combined_all_years_athletes_lower.filter(
		combined_all_years_athletes_lower.country.isin(["china","india","usa"]))
		
medal_and_points_y = medal_and_points_df.withColumnRenamed('year','y')
		
china_india_usa = china_india_usa.orderBy("country", ascending=True) #ordered df
china_india_usa = china_india_usa.select("id","name","sport","country","coach_id",'y') ##only required fields
china_india_usa_with_points = china_india_usa.join(medal_and_points_y, on=["id","sport",'y'], how="inner").distinct()
china_india_usa_with_points = china_india_usa_with_points.orderBy(F.col("country").asc(), F.col("coach_id").asc()) ##sorted
#china_india_usa_with_points.show()

##retain only those players whose sport matches coach sport using join
china_india_usa_coach_matches = china_india_usa_with_points.join(coaches_df,
			(china_india_usa_with_points.coach_id == coaches_df.id) &
			(china_india_usa_with_points.sport == coaches_df.sport),
			"inner").select(china_india_usa_with_points["*"])
##china_india_usa_coach_matches.show()

coach_total_points_df = china_india_usa_coach_matches.groupBy("coach_id","country") \
				.agg(F.sum("points").alias("coach_total"),
					F.sum("gold_medals").alias("golds"),
					F.sum("silver_medals").alias("silvers"),
					F.sum("bronze_medals").alias("bronzes"))
##coach_total_points_df.show()

coach_total_points_df = coach_total_points_df.orderBy(F.col("country").asc(), F.col("coach_total").desc())
coach_total_points_df = coach_total_points_df.join(coaches_df.select("id","name"),
							coach_total_points_df.coach_id == coaches_df.id,
							"inner")				
##coach_total_points_df.show()

window_task2 = Window.partitionBy("country").orderBy(F.col("coach_total").desc(),
						F.col("golds").desc(),
						F.col("silvers").desc(),
						F.col("bronzes").desc(),
						F.col("name").asc())
					
coaches_ranking = coach_total_points_df.withColumn("rank",F.rank().over(window_task2))
top_five_coaches_per_country = coaches_ranking.filter(F.col("rank")<=5)
top_five_coaches_per_country = top_five_coaches_per_country.orderBy(F.col("country").asc())
##top_five_coaches_per_country.show()

##need to alias because of column ambiguity
top_five_coaches_per_country = top_five_coaches_per_country.alias("top")
coaches_df = coaches_df.alias("cd")

##need coach names so join 

req_coach_names = top_five_coaches_per_country.join(coaches_df,
				top_five_coaches_per_country.coach_id == coaches_df.id, "inner").select(
				F.col("top.coach_id"),
				F.col("cd.name"),
				F.col("top.country"),
				F.col("top.rank")).distinct()
		
##req_coach_names.show()
list_of_top_coaches = [row.name.upper() for row in req_coach_names.collect()]
main_output = (list_of_top_performers, list_of_top_coaches)
with open(sys.argv[6], 'w') as f:
    f.write(str(main_output).replace("'", '"'))


spark.stop()
