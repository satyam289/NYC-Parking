from pyspark.sql.functions import count,desc
import matplotlib.pyplot as plt

def violation_frequencey(data_frame, enable_plot=True):
    #violation_count = 0
    violation_count = data_frame.select('violation_code')\
                                .groupBy('violation_code')\
                                .agg(count('violation_code')\
                                .alias('no_of_tickets'))\
                                .sort(desc('no_of_tickets'))
    # plot top 5 Code Violation
    q3_for_plot = violation_count.toPandas()

    if enable_plot:
      ax = q3_for_plot.head(10).plot.bar(x='violation_code', y='no_of_tickets', figsize=(10, 5))
      ax.set_title("Violation Code vs No. of violations")
      ax.set_xlabel("Violation Code")
      ax.set_ylabel("No. of Violations")
      fig = ax.get_figure()
      fig.savefig('../output/violation_frequencey.png')

    return q3_for_plot

def violations_by_bodytype(data_frame, enable_plot=True):
    #body_type = 0
    body_type  = data_frame.select('vehicle_body_type')\
                              .groupBy('vehicle_body_type')\
                              .agg(count('vehicle_body_type')\
                              .alias('Ticket_Frequency'))\
                              .sort(desc('Ticket_Frequency'))
    # plot Violations on the basis of Vehicle_Body_Type
    vehicleBodyType_for_plot = body_type.toPandas()

    if enable_plot:
      ax = vehicleBodyType_for_plot.head(10).plot.bar(x='vehicle_body_type', y='Ticket_Frequency', figsize=(10, 5))
      ax.set_title("Vehicle Type vs No. of violations")
      ax.set_xlabel("Vehicle Type")
      ax.set_ylabel("No. of Violations")
      fig = ax.get_figure()
      fig.savefig('../output/violations_by_bodytype.png')

    return vehicleBodyType_for_plot

def violations_by_make(data_frame, enable_plot=True):
    make_type  = data_frame.select('vehicle_make')\
                              .groupBy('vehicle_make')\
                              .agg(count('vehicle_make')\
                              .alias('Ticket_Frequency'))\
                              .sort(desc('Ticket_Frequency'))
    # plot Violations on the basis of Vehicle_Make
    vehicleMake_for_plot = make_type.toPandas()

    if enable_plot:
      ax = vehicleMake_for_plot.head(10).plot.bar(x='vehicle_make',  y='Ticket_Frequency', figsize=(10, 5))
      ax.set_title("Vehicle Make vs No. of violations")
      ax.set_xlabel("Vehicle Make")
      ax.set_ylabel("No. of Violations")
      fig = ax.get_figure()
      fig.savefig('../output/violations_by_make.png')

    return vehicleMake_for_plot
