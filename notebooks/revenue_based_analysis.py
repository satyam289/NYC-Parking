from pyspark.sql.functions import count, desc
import pandas as pd

def yearly_revenue(data_frame, enable_plot=True):
    years = ["2017", "2018", "2019", "2020", "2021"]
    dict_df = {'year': [], "revenue": []}
    for year in years:
        violation_count = get_violation_df__yearly(data_frame, year)
        revenue = accumulatedTax_per_Violation(violation_count)['cost'].sum()
        dict_df['year'].append(year)
        dict_df['revenue'].append(revenue)

    yearly_revenue_pd = pd.DataFrame(dict_df)

    if enable_plot:
      ax = yearly_revenue_pd.head(10).plot.bar(x='year', y='revenue', figsize=(10, 5))
      ax.set_title("Year vs Revenue ($)")
      ax.set_xlabel("Year")
      ax.set_ylabel("Revenue ($)")
      fig = ax.get_figure()
      fig.savefig('../output/yearly_revenue.png')

    return yearly_revenue_pd

def highest_revenue(data_frame, enable_plot=True):
    violation_count =  get_violation_df__yearly(data_frame, "")
    highest_revenue_pd = accumulatedTax_per_Violation(violation_count)

    if enable_plot:
      ax = highest_revenue_pd.head(10).plot.bar(x='violation_code', y='cost', figsize=(10, 5))
      ax.set_title("Violation Code vs Revenue ($)")
      ax.set_xlabel("Violation Code")
      ax.set_ylabel("Revenue ($)")
      fig = ax.get_figure()
      fig.savefig('../output/highest_revenue.png')

    return highest_revenue_pd

def calulateTax(violation_code, frequency, dict_map):
    price_rate = dict_map.get(int(violation_code))
    return price_rate * frequency

def accumulatedTax_per_Violation(df):
    df = df.toPandas()
    price_df = read_priceTag_violation_csv()
    dic_map = dict(zip(price_df.violation_code, price_df.Fine))
    df['cost'] = df.apply(lambda x: calulateTax(1, x['no_of_tickets'], dic_map), axis=1)
    df = df[['violation_code', 'cost']]
    sorted_df = df.sort_values(by=['cost'], ascending=False)
    return sorted_df

def read_priceTag_violation_csv():
    price_df = pd.read_excel(r'../docs/ParkingViolationCodes_January2020.xlsx')
    dict = {'VIOLATION CODE': 'violation_code','All Other Areas\n(Fine Amount $)': 'Fine'}
    price_df.rename(columns=dict, inplace=True)
    return price_df

def get_violation_df__yearly(df, year):
    return df.select('violation_code', 'issue_date')\
        .filter(df['issue_date'].rlike(year + "-*"))\
        .groupBy('violation_code')\
        .agg(count('violation_code')
             .alias('no_of_tickets'))\
        .sort(desc('no_of_tickets'))
