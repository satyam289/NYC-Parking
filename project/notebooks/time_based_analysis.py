
# importing package
import matplotlib.pyplot as plt
import numpy as np  
import pandas as pd


def three_most_common_violations_in_6_time_bins (spark, print_enable = False):

    #NYCPV is the SQL view cretaed using the dataframe. The dataframe is created with the same below spark session.
    #So the life cycle of the view is till the life cycle of spark session object.

    #Find any null values exist in the column voilation time. Find the count and also the rows.
    vltime_null_count_df = spark.sql("SELECT count(*) as No_of_Count_Values from NYCPV WHERE violation_time is NULL")
    #vltime_null_df = spark.sql("SELECT * from NYCPV WHERE violation_time is NULL")

    #Find any NOT null values exist in the column voilation time. Find the count and also the rows.
    #vltime_no_null_count_df = spark.sql("SELECT count(*) as No_of_Count_Values from NYCPV WHERE violation_time is NOT NULL")
    vltime_no_null_df = spark.sql("SELECT * from NYCPV WHERE violation_time is NOT NULL")

    #Build new SQL view with not null values for voilation time
    vltime_no_null_df.createOrReplaceTempView("NYCPV_VT_NN")

    #Find any null values exist in the column voilation time. Find the count and also the rows.
    #vltime_null_count_df_new = spark.sql("SELECT count(*) as No_of_Count_Values from NYCPV_VT_NN WHERE violation_time is NULL")
    #vltime_null_df_new = spark.sql("SELECT * from NYCPV_VT_NN WHERE violation_time is NULL")


    
    # Divide 24 hours into six equal discrete bins of time.
    # Bin       Time Interval
    # 1         12:00 AM to 4:00 AM
    # 2         4:00 AM to 8:00 AM
    # 3         8:00 AM to 12:00 PM
    # 4         12:00 PM to 4:00 PM
    # 5         4:00 PM to 8:00 PM
    # 6         8:00 PM to 12:00 AM
    df_with_time_bins=spark.sql("SELECT summons_number, violation_code , violation_time, issuer_precinct, \
         case when substring(violation_time,1,2) in ('00','01','02','03','12') \
         and upper(substring(violation_time,-1))='A' then 1 \
         when substring(violation_time,1,2) in ('04','05','06','07') \
         and upper(substring(violation_time,-1))='A' then 2 \
         when substring(violation_time,1,2) in ('08','09','10','11') \
         and upper(substring(violation_time,-1))='A' then 3 \
         when substring(violation_time,1,2) in ('12','00','01','02','03') \
         and upper(substring(violation_time,-1))='P' then 4 \
         when substring(violation_time,1,2) in ('04','05','06','07') \
         and upper(substring(violation_time,-1))='P' then 5 \
         when substring(violation_time,1,2) in ('08','09','10','11') \
         and upper(substring(violation_time,-1))='P' then 6 \
         else null end as violation_time_bin \
         from NYCPV_VT_NN where violation_time is not null or (length(violation_time)=5 \
         and upper(substring(violation_time,-1)) in ('A','P') \
         and substring(violation_time,1,2) in ('00','01','02','03','04','05','06','07', '08','09','10','11','12'))")
    
    #Create new sql view with time bins for further processing.
    df_with_time_bins.createOrReplaceTempView("NYCPV_VT_NN_TB")

    #select voilation code and bin from sql view which we need for further identifying 3 most common violation codes.
    violation_code_time_count_df = spark.sql("SELECT violation_code,violation_time_bin, count(*) count \
                                              from NYCPV_VT_NN_TB group by violation_code,violation_time_bin")

    #Create Seperate dataframes for each time bin and group them with Voilation code and count in descending order, so we can pick top 3.
    violation_code_count_bin_pd_1 = spark.sql("select violation_time_bin,violation_time_bin violation_time,violation_code,count(*) violation_count \
                                               from NYCPV_VT_NN_TB where violation_time_bin == 1 \
                                               group by violation_time_bin,violation_code order by violation_count desc").limit(3).toPandas()
    violation_code_count_bin_pd_2 = spark.sql("select violation_time_bin,violation_time_bin violation_time,violation_code,count(*) violation_count \
                                               from NYCPV_VT_NN_TB where violation_time_bin == 2 \
                                               group by violation_time_bin,violation_code order by violation_count desc").limit(3).toPandas()
    violation_code_count_bin_pd_3 = spark.sql("select violation_time_bin,violation_time_bin violation_time,violation_code,count(*) violation_count \
                                               from NYCPV_VT_NN_TB where violation_time_bin == 3 \
                                               group by violation_time_bin,violation_code order by violation_count desc").limit(3).toPandas()
    violation_code_count_bin_pd_4 = spark.sql("select violation_time_bin,violation_time_bin violation_time,violation_code,count(*) violation_count \
                                               from NYCPV_VT_NN_TB where violation_time_bin == 4 \
                                               group by violation_time_bin,violation_code order by violation_count desc").limit(3).toPandas()
    violation_code_count_bin_pd_5 = spark.sql("select violation_time_bin,violation_time_bin violation_time,violation_code,count(*) violation_count \
                                               from NYCPV_VT_NN_TB where violation_time_bin == 5 \
                                               group by violation_time_bin,violation_code order by violation_count desc").limit(3).toPandas()
    violation_code_count_bin_pd_6 = spark.sql("select violation_time_bin,violation_time_bin violation_time,violation_code,count(*) violation_count \
                                               from NYCPV_VT_NN_TB where violation_time_bin == 6 \
                                               group by violation_time_bin,violation_code order by violation_count desc").limit(3).toPandas()
    

    # Divide 24 hours into six equal discrete bins of time.
    # Bin       Time Interval
    # 1         12:00 AM to 4:00 AM
    # 2         4:00 AM to 8:00 AM
    # 3         8:00 AM to 12:00 PM
    # 4         12:00 PM to 4:00 PM
    # 5         4:00 PM to 8:00 PM
    # 6         8:00 PM to 12:00 AM
    time_bin_to_time = {1:'12AM-4AM', \
                        2:'4AM-8AM', \
                        3:'8AM-12PM', \
                        4:'12PM-4PM', \
                        5:'4PM-8PM', \
                        6:'8PM-12AM'}

    violation_code_count_bin_pd_1['violation_time'].replace(time_bin_to_time, inplace=True)
    violation_code_count_bin_pd_2['violation_time'].replace(time_bin_to_time, inplace=True)
    violation_code_count_bin_pd_3['violation_time'].replace(time_bin_to_time, inplace=True)
    violation_code_count_bin_pd_4['violation_time'].replace(time_bin_to_time, inplace=True)
    violation_code_count_bin_pd_5['violation_time'].replace(time_bin_to_time, inplace=True)
    violation_code_count_bin_pd_6['violation_time'].replace(time_bin_to_time, inplace=True)

    
    if print_enable:
        #vltime_null_count_df.show(5)
        #vltime_null_df.show(5)
        #vltime_no_null_count_df.show(5)
        #vltime_no_null_df.show(5)
        #vltime_null_count_df_new.show(5)
        #vltime_null_df_new.show(5)
        #df_with_time_bins.show()
        #violation_code_time_count_df.show()

        #Now Pick up to 3 violation codes which are descending order from df, 
        print(violation_code_count_bin_pd_1)
        print(violation_code_count_bin_pd_2)
        print(violation_code_count_bin_pd_3)
        print(violation_code_count_bin_pd_4)
        print(violation_code_count_bin_pd_5)
        print(violation_code_count_bin_pd_6)



def five_most_common_Violations_with_times(spark, print_enable = False):

    #Already NYCPV_VT_NN_TB sql view with time bins is created in previous method.
    # The SQL view will be available in spark session for further processing. Reuse the same.
      

    #select voilation code and bin from sql view which we need for further identifying 3 most common violation codes.
    violation_code_time_count_df = spark.sql("SELECT violation_code,violation_time_bin, violation_time_bin violation_time,  count(*) voilation_count \
                                              from NYCPV_VT_NN_TB group by violation_code,violation_time_bin \
                                              order by voilation_count desc")                                         


    violation_code_time_count_pd = violation_code_time_count_df.limit(5).toPandas()

    # Divide 24 hours into six equal discrete bins of time.
    # Bin       Time Interval
    # 1         12:00 AM to 4:00 AM
    # 2         4:00 AM to 8:00 AM
    # 3         8:00 AM to 12:00 PM
    # 4         12:00 PM to 4:00 PM
    # 5         4:00 PM to 8:00 PM
    # 6         8:00 PM to 12:00 AM
    time_bin_to_time = {1:'12AM-4AM', \
                        2:'4AM-8AM', \
                        3:'8AM-12PM', \
                        4:'12PM-4PM', \
                        5:'4PM-8PM', \
                        6:'8PM-12AM'}
    violation_code_time_count_pd['violation_time'].replace(time_bin_to_time, inplace=True)

    violation_code_list = violation_code_time_count_pd["violation_code"].tolist()
    violation_time_bin_list = violation_code_time_count_pd["violation_time_bin"].tolist()
    violation_time_list = violation_code_time_count_pd["violation_time"].tolist()

    five_most_common_Voilations_with_times_list = [str(violation_code_list[i]) + \
                                                   ", " + str(violation_time_list[i]) \
                                                   for i in range(len(violation_code_list))]
    voilation_count_list = violation_code_time_count_pd["voilation_count"].tolist()

       

    if print_enable:
        print(violation_code_time_count_pd)
        fig = plt.figure(figsize = (10, 5))

        # creating the bar plot
        plt.bar(five_most_common_Voilations_with_times_list, voilation_count_list, width = 0.4)

        plt.xlabel("ViolationCode, TIme")
        plt.ylabel("Violation Count")
        plt.title("Five Most Common Voilations With Times")

        plt.savefig("../output/five_most_common_violations_with_times.png")
        plt.show()