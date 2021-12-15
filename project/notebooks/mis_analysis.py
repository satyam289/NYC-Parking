#  in-state vs. out of state, impact of holidays/long weekends, repeat offenders, etc.
def repeat_offenders(spark, enable_plot=True):

  repeat_offenders = spark.sql("\
                                select plate_id, count(plate_id) as no_of_violations \
                                from NYCPV \
                                group by plate_id \
                                order by no_of_violations desc"
                              )

  offenders = repeat_offenders.toPandas()

  if enable_plot:
    ax = offenders.head(10).plot.bar(x='plate_id', y='no_of_violations', figsize=(10, 5))
    ax.set_title("Plate ID vs No. of violations")
    ax.set_xlabel("Plate ID")
    ax.set_ylabel("No. of Violations")
    fig = ax.get_figure()
    fig.savefig('../output/repeat_offenders.png')

  return offenders


def in_out_state(spark, enable_plot=True):

  in_out_state = spark.sql("\
                            select registration_state, count(registration_state) as no_of_violations \
                            from NYCPV \
                            group by registration_state \
                            order by no_of_violations desc"
                          )

  in_out_state_PD = in_out_state.toPandas()

  if enable_plot:
    ax = in_out_state_PD.head(10).plot.bar(x='registration_state', y='no_of_violations', figsize=(10, 5))
    ax.set_title("State vs No. of violations")
    ax.set_xlabel("State")
    ax.set_ylabel("No. of Violations")
    fig = ax.get_figure()
    fig.savefig('../output/in_out_state.png')

  return in_out_state_PD


def weekends(spark, enable_plot=True):

  weekends = spark.sql("\
                        select dayofweek(issue_date) as week, count(summons_number) as no_of_violations \
                        from NYCPV \
                        group by week \
                        order by no_of_violations desc"
                      )
  weekends_PD = weekends.toPandas()
  week_day = {1:'Sunday', 2:'Monday', 3:'Tuesday', 4:'Wednesday', 5:'Thursday', 6:'Friday', 7:'Saturday'}
  weekends_PD['week'].replace(week_day, inplace=True)

  if enable_plot:
    ax = weekends_PD.head(10).plot.bar(x='week', y='no_of_violations', figsize=(10, 5))
    ax.set_title("Week vs No. of violations")
    ax.set_xlabel("Week")
    ax.set_ylabel("No. of Violations")
    fig = ax.get_figure()
    fig.savefig('../output/weekends.png')

  return weekends_PD

def holidays(spark, enable_plot=True):
  holidays = spark.sql("\
                        select concat(day(issue_date), '-', month(issue_date)) as day, count(summons_number) as no_of_violations \
                        from NYCPV \
                        group by day \
                        order by no_of_violations"
                      )
  holidays_PD = holidays.toPandas()

  if enable_plot:
    ax = holidays_PD.head(10).plot.bar(x='day', y='no_of_violations', figsize=(10, 5))
    ax.set_title("Day vs No. of violations")
    ax.set_xlabel("Day-Month")
    ax.set_ylabel("No. of Violations")
    fig = ax.get_figure()
    fig.savefig('../output/holidays.png')

  return holidays_PD