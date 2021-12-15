from pyspark.sql.functions import col
import matplotlib.pyplot as plt

def violating_precicts(nyc_data, enable_plot=True):
    nyc_precints = nyc_data.select('violation_precinct')\
                           .filter(col('violation_precinct') != 0)\
                           .groupBy('violation_precinct')\
                           .agg({'violation_precinct':'count'})\
                           .withColumnRenamed("count(violation_precinct)", "no_of_violations")\
                           .sort('no_of_violations', ascending=False)

    nyc_precints_pd =nyc_precints.toPandas()

    if enable_plot:
        ax = nyc_precints_pd.head(10).plot.bar(x='violation_precinct', y='no_of_violations', figsize=(10, 5))
        ax.set_title("Police Precinct Zone vs No. of violations")
        ax.set_xlabel("Police Precinct Zone")
        ax.set_ylabel("No. of Violations")
        fig = ax.get_figure()
        fig.savefig('../output/violating_precicts.png')

    return nyc_precints_pd

def issuing_precincts(nyc_data, enable_plot=True):
    nyc_precints = nyc_data.select('issuer_precinct')\
                           .filter(col('issuer_precinct') != 0)\
                           .groupBy('issuer_precinct')\
                           .agg({'issuer_precinct':'count'})\
                           .withColumnRenamed("count(issuer_precinct)", "no_of_violations")\
                           .sort('count(issuer_precinct)', ascending=False)

    nyc_precints_pd =nyc_precints.toPandas()

    if enable_plot:
        ax = nyc_precints_pd.head(10).plot.bar(x='issuer_precinct', y='no_of_violations', figsize=(10, 5))
        ax.set_title("Police Precinct vs No. of issued violations")
        ax.set_xlabel("Police Precinct")
        ax.set_ylabel("No. of issued violations")
        fig = ax.get_figure()
        fig.savefig('../output/issuing_precincts.png')

    return nyc_precints_pd

def violation_code_frequency_top3_precincts(nyc_data, enable_plot=True):
    top3_precints = nyc_data.filter(col('violation_code') != 0)\
                           .filter(col('issuer_precinct') != 0)\
                           .select('issuer_precinct')\
                           .groupBy('issuer_precinct')\
                           .agg({'issuer_precinct':'count'})\
                           .sort('count(issuer_precinct)', ascending=False)\
                           .take(3)
    top3 = [row['issuer_precinct'] for row in top3_precints]
    filtered_data = nyc_data.filter((col('issuer_precinct') == top3[0]) | (col('issuer_precinct') == top3[1]) | (col('issuer_precinct') == top3[2]))
    violation_frequencies_df = filtered_data.select('violation_code')\
                                         .groupBy('violation_code')\
                                         .agg({'violation_code':'count'})\
                                         .withColumnRenamed('count(violation_code)', 'Freq of Violations')\
                                         .sort('Freq of Violations', ascending=False)

    violation_frequencies = violation_frequencies_df.collect()

    if enable_plot:
        violations = [row['violation_code'] for row in violation_frequencies]
        frequencies = [row['Freq of Violations'] for row in violation_frequencies]

        fig, ax = plt.subplots(1, 1, figsize=(10,5))
        ax.set_title("Violation Code Vs No. of violations of Top 3 precincts (ticket issue wise)")
        ax.set_xlabel("Violation Code")
        ax.set_ylabel("o. of violations")
        ax.bar(violations[:10], frequencies[:10])

        fig.savefig('../output/violation_code_frequency_top3_precincts.png')
        print("Top 3 Violating Precicts :", top3)

    return violation_frequencies_df.toPandas()
