# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

# %% [markdown]
# ## Pre-processing Functions
#
# - All years data has same 43 columns
# - We have **Fiscal year data (July 1 - June 30)**. That means one file contains data for 2 years
# - **Issue Date:** We have to remove all the records which are outside of above condition
# - **Violation Code:** Codes other than those between 1 and 99
# - **Registration state:** has 99 invalid state code *(Take care in the analysis part)*
# - **Plate Type:** has more than 3 letters code. We have to remove them *(Take care in the analysis part)*
# - **Violation Time:** There are some times without P & A *(Take care in the analysis part)*

# %%
import pyspark.sql.functions as F
from pyspark.sql.functions import col

# %% [markdown]
# ### Removing the columns which are not used in our analysis

# %%
def remove_unused_columns(df, used_columns):

  print('\n################ Removing the columns which are not used in our analysis #######################')
  # Not used columns
  # "Issuing Agency", "Street Code1", "Street Code2", "Street Code3", "Vehicle Expiration Date", "Violation Location", "Issuer Code", "Issuer Command", "Issuer Squad", "Time First Observed", "Violation County", "Violation In Front Of Or Opposite", "House Number", "Street Name", "Intersecting Street", "Date First Observed", "Law Section", "Sub Division", "Violation Legal Code", "Days Parking In Effect    ", "From Hours In Effect", "To Hours In Effect", "Unregistered Vehicle?", "Vehicle Year", "Meter Number", "Feet From Curb", "Violation Post Code", "No Standing or Stopping Violation", "Hydrant Violation", "Double Parking Violation"

  all_columns = set(df.columns)
  used_columns = set(used_columns)
  unused_columns = all_columns - used_columns

  df = df.drop(*unused_columns)

  print(f'\nNo. of Columns (before dropping columns) : {len(all_columns)}')
  print(f'No. of Columns (after dropping columns) : {len(df.columns)}')

  return df

# %% [markdown]
# ### Dropping Duplicate Rows

# %%
def drop_duplicates_nulls(df):

  print('\n########################## Dropping Duplicate & Null Rows ######################################')
  before = df.count()

  df = df.na.drop("all") # Drops the rows which are having null in all the columns
  df = df.drop_duplicates()

  print(f'No. of Records (before dropping duplicates & NUlls) : {before}')
  print(f'No. of Records (after dropping duplicates & NULLs) : {df.count()}')

  return df

# %% [markdown]
# ### Converting column names to lower case & replacing spaces with _

# %%
def santize_column_names(df):

  print('\n############## Converting column names to lower case & replacing spaces with "_" ###############')
  before = df.columns

  ## Slow
  # columns = [column.lower().replace(" ", "_") for column in df.columns ]
  # df = df.toDF(*columns)

  df = df.select([col(c).alias(c.lower().replace(" ", '_')) for c in df.columns])

  print(f'\nColumns (before sanitizing column names) : {before}')
  print(f'\nColumns (after sanitizing column names) : {df.columns}')

  return df

# %% [markdown]
# ### Ensure all the values in a column are unique

# %%
def assert_uniqueness(df, column_name):

  print(f"\n################# Ensure all the values in '{column_name}' column are unique ##################")

  all_rows      = df.select(column_name).count()
  distinct_rows = df.select(column_name).distinct().count()

  assert all_rows == distinct_rows

  print(f"All values in {column_name} column are unique")

  return True

# %% [markdown]
# ### Converting issue date string type to Date type

# %%
def convert_to_date(df, column_name, format):

  print('\n###################### Converting issue date string type to Date type ##########################')

  df = (
        df
        .withColumn(
          column_name,
          F.to_date(col(column_name), format)
        )
      )

  return df

# %% [markdown]
# ### Removing the rows which are outside of the passed years

# %%
def remove_outside_years_data(df, years, column_name):

  print('\n################### Removing the rows which are outside of the passed years ####################')

  print(f'\nDistinct years in {column_name} (before removing) :')
  (
    df
    .select(F.year(col(column_name)).alias("year"), 'summons_number')
    .groupBy("year")
    .agg(F.count("summons_number").alias("No. of violations"))
    .sort("year")
    .show()
  )

  min_year = min(years)
  max_year = max(years)

  df = (
        df
        .select('*')
        .where(
          (F.year(col(column_name)) >= min_year)
          &
          (F.year(col(column_name)) <= max_year)
        )
      )

  print(f'Distinct years in {column_name} (after removing data outside of 2017-2021) : ')
  df.select(F.year(col(column_name))).distinct().show()

  return df

# %% [markdown]
# ### Removing violation codes other than those between 1 and 99

# %%
def remove_invalid_violation_code_data(df):

  print(f'Distinct violation codes (before removing) : {df.select("violation_code").distinct().count()}')

  df = (
        df
        .select('*')
        .where((col("violation_code") >= 1) & (col("violation_code") <= 99))
      )

  print(f'Distinct violation codes (before removing) : {df.select("violation_code").distinct().count()}')
  print('\n###########################################################################')

  return df

# %% [markdown]
# ### Generate schema string

# %%
def get_schema(schema_columns, schema_types):
  schema = []
  for col in schema_columns:
    schema_str = f"`{col}` "
    if col in schema_types:
      schema_str += f"{schema_types[col]['type']} "
      schema_str += "NOT NULL" if not schema_types[col]['null'] else ""
    else:
      schema_str += "string"
    schema.append(schema_str)

  return ','.join(schema)

