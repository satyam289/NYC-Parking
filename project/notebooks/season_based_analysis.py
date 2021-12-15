from pyspark.sql.functions import expr, col, lit, year
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def violations_in_year(nyc_data, vyear):
    return nyc_data.select('issue_date').filter(year('issue_date') == vyear).count()

def reduction_in_violations(nyc_data, enable_plot=True):
    violations_2018 = violations_in_year(nyc_data, 2018)
    violations_2019 = violations_in_year(nyc_data, 2019)
    violations_2020 = violations_in_year(nyc_data, 2020)

    years = [2018, 2019, 2020]
    violations = [violations_2018, violations_2019, violations_2020]

    if enable_plot:
        fig, ax = plt.subplots(1, 1, figsize=(10,5))
        ax.set_title("Year Vs No. of violations")
        ax.set_xlabel("Year")
        ax.set_ylabel("No. of violations")
        ax.bar(years, violations, color='blue')

        fig.savefig('../output/reduction_in_violations.png')

    reduction_2019_2018 = ((violations_2018 - violations_2019)/violations_2018) * 100
    reduction_2020_2018 = ((violations_2019 - violations_2020)/violations_2019) * 100

    reduction_data = [('Reduction 2019 from 2018', reduction_2019_2018), ('Reduction 2020 from 2019',reduction_2020_2018)]
    reduction_pad = pd.DataFrame(reduction_data, columns = ['Reduction Years', 'Reduction'])
    return reduction_pad

def season_violation_frequencies(nyc_data, enable_plot=True):
    season_bins = nyc_data.withColumn('season', expr("case when month(issue_date) in (12, 1, 2) then 'winter'\
                                                           when month(issue_date) in (3, 4, 5) then 'spring' \
                                                           when month(issue_date) in (6, 7, 8) then 'summer'\
                                                           when month(issue_date) in (9, 10, 11) then 'autumn'\
                                                      end"))
    season_freq = season_bins.select('season').groupBy('season').agg({'season': 'count'}).withColumnRenamed('count(season)', 'No of tickets')

    ## plot bar graph for seasons
    season_freq_data = season_freq.collect()
    seasons = [row['season'] for row in season_freq_data]
    frequencies = [row['No of tickets'] for row in season_freq_data]

    if enable_plot:
        fig, ax = plt.subplots(1, 1, figsize=(10, 5))
        ax.set_title("Season Vs No. of violations")
        ax.set_xlabel("Season")
        ax.set_ylabel("No. of violations")
        ax.bar(seasons, frequencies, color=['green', 'cyan','yellow'])
        fig.savefig('../output/season_violation_frequencies.png')

    return season_freq.toPandas()

def season_violations(season_bins, season):
    violation_by_season = season_bins.select('season', 'violation_code')\
                                  .filter(col('season') == season)\
                                  .groupBy('violation_code')\
                                  .agg({'violation_code':'count'})\
                                  .withColumnRenamed('count(violation_code)', 'Freq of Violations')\
                                  .withColumn('season', lit(season))\
                                  .sort('Freq of Violations', ascending=False)\
                                  .take(3)

    return violation_by_season

def common_violations_season(nyc_data, enable_plot=True):
    season_bins = nyc_data.withColumn('season', expr("case when month(issue_date) in (12, 1, 2) then 'winter'\
                                                           when month(issue_date) in (3, 4, 5) then 'spring' \
                                                           when month(issue_date) in (6, 7, 8) then 'summer'\
                                                           when month(issue_date) in (9, 10, 11) then 'autumn'\
                                                      end"))

    spring_violations = season_violations(season_bins, 'spring')
    winter_violations = season_violations(season_bins, 'winter')
    summer_violations = season_violations(season_bins, 'summer')
    autumn_violations = season_violations(season_bins, 'autumn')

    spring_v = [row['Freq of Violations'] for row in spring_violations]
    spring_l = [row['violation_code'] for row in spring_violations]

    winter_v = [row['Freq of Violations'] for row in winter_violations]
    wintert_l = [row['violation_code'] for row in winter_violations]

    summer_v = [row['Freq of Violations'] for row in summer_violations]
    summer_l = [row['violation_code'] for row in summer_violations]

    autumn_v = [row['Freq of Violations'] for row in autumn_violations]
    autumn_l = [row['violation_code'] for row in autumn_violations]

    if enable_plot:
        x_ticks = ['spring', 'winter', 'summer', 'autumn']
        labels = ['Top1', 'Top2', 'Top3']
        x = np.arange(len(labels))
        width = 0.2
        fig, ax = plt.subplots(1, 1, figsize=(10,5))
        ax.bar(x-0.2, spring_v, width, color='cyan')
        ax.bar(x, winter_v, width, color='orange')
        ax.bar(x+0.2, summer_v, width, color='green')
        ax.bar(x+0.4, autumn_v, width, color='yellow')

        ax.set_xlabel("Seasons")
        ax.set_ylabel("Violations")
        ax.legend(["spring", "winter", "summer", "autumn"])

        fig.savefig('../output/common_violations_season.png')

    seasonwise_violations = spring_violations + winter_violations + summer_violations + autumn_violations
    pd_seasonwise_violations = pd.DataFrame(seasonwise_violations, columns = ['Violation Code', 'Frequency', 'Season'])

    return pd_seasonwise_violations
