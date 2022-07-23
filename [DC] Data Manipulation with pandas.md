# [DC] Data Manipulation with pandas

2022/07

[Datacamp course - Data Manipulation with pandas](https://app.datacamp.com/learn/courses/data-manipulation-with-pandas)
[my notes (hackmd ver.)](https://hackmd.io/dmyB1Xi6RTy5uMn014CNoQ?view)


:::info
[TOC]
:::


## 1. Transforming DataFrames
Let’s master the pandas basics. Learn how to inspect DataFrames and perform fundamental manipulations, including sorting rows, subsetting, and adding new columns.

### --- Introducing DataFrames ---
![](https://i.imgur.com/b0TvvMM.png)

- `.head()` returns the first few rows (the “head” of the DataFrame).
- `.info()` shows information on each of the columns, such as the data type and number of missing values.
- `.shape` returns the number of rows and columns of the DataFrame.
- `.describe()` calculates a few summary statistics for each column.

### Inspecting a DataFrame

```python
# Print the head of the homelessness data
print(homelessness.head())
```
```
               region       state  individuals  family_members  state_pop
0  East South Central     Alabama       2570.0           864.0    4887681
1             Pacific      Alaska       1434.0           582.0     735139
2            Mountain     Arizona       7259.0          2606.0    7158024
3  West South Central    Arkansas       2280.0           432.0    3009733
4             Pacific  California     109008.0         20964.0   39461588
```

```python
# Print information about homelessness
print(homelessness.info())
```
```
# <class 'pandas.core.frame.DataFrame'>
# Int64Index: 51 entries, 0 to 50
# Data columns (total 5 columns):
#  #   Column          Non-Null Count  Dtype  
# ---  ------          --------------  -----  
#  0   region          51 non-null     object 
#  1   state           51 non-null     object 
#  2   individuals     51 non-null     float64
#  3   family_members  51 non-null     float64
#  4   state_pop       51 non-null     int64  
# dtypes: float64(2), int64(1), object(2)
# memory usage: 2.4+ KB
# None
```


```python
# Print the shape of homelessness
print(homelessness.shape)
# (51, 5)
```

```python
# Print a description of homelessness
print(homelessness.describe())
```
```
individuals  family_members  state_pop
count       51.000          51.000  5.100e+01
mean      7225.784        3504.882  6.406e+06
std      15991.025        7805.412  7.327e+06
min        434.000          75.000  5.776e+05
25%       1446.500         592.000  1.777e+06
50%       3082.000        1482.000  4.461e+06
75%       6781.500        3196.000  7.341e+06
max     109008.000       52070.000  3.946e+07
```


### Parts of a DataFrame
- `.values`: A two-dimensional NumPy array of values.
- `.columns`: An index of columns: the column names.
- `.index`: An index for the rows: either row numbers or row names.

```python
# Import pandas using the alias pd
import pandas as pd

# Print the values of homelessness
print(homelessness.values)

# Print the column index of homelessness
print(homelessness.columns)

# Print the row index of homelessness
print(homelessness.index)
```

```
[['East South Central' 'Alabama' 2570.0 864.0 4887681]
 ['Pacific' 'Alaska' 1434.0 582.0 735139]
 ['Mountain' 'Arizona' 7259.0 2606.0 7158024]
  ...
 ['South Atlantic' 'West Virginia' 1021.0 222.0 1804291]
 ['East North Central' 'Wisconsin' 2740.0 2167.0 5807406]
 ['Mountain' 'Wyoming' 434.0 205.0 577601]]
---
Index(['region', 'state', 'individuals', 'family_members', 'state_pop'], dtype='object')
---
Int64Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50], dtype='int64')
```

### Sorting and subsetting
Sorting
![](https://i.imgur.com/wvaDYTq.png)
True, False不用引號
![](https://i.imgur.com/YtfqA78.png)
subsetting
![](https://i.imgur.com/6djueOh.png)
要雙中括號
![](https://i.imgur.com/WUy5DIm.png)
![](https://i.imgur.com/8MyadOu.png)
![](https://i.imgur.com/xTQ8sCJ.png)
![](https://i.imgur.com/GcWkWys.png)
![](https://i.imgur.com/LkLgnc1.png)
![](https://i.imgur.com/wLIqJRz.png)

### --- Sorting rows ---

ex1
```python
# Sort homelessness by individuals
homelessness_ind = homelessness.sort_values("individuals")

# Print the top few rows
print(homelessness_ind.head())

#                 region         state  individuals  family_members  state_pop
# 50            Mountain       Wyoming        434.0           205.0     577601
# 34  West North Central  North Dakota        467.0            75.0     758080
# 7       South Atlantic      Delaware        708.0           374.0     965479
# 39         New England  Rhode Island        747.0           354.0    1058287
# 45         New England       Vermont        780.0           511.0     624358
```
ex2
```python
# Sort homelessness by descending family members
homelessness_fam = homelessness.sort_values("family_members", ascending=False)

# Print the top few rows
print(homelessness_fam.head())
#                 region          state  individuals  family_members  state_pop
# 32        Mid-Atlantic       New York      39827.0         52070.0   19530351
# 4              Pacific     California     109008.0         20964.0   39461588
# 21         New England  Massachusetts       6811.0         13257.0    6882635
# 9       South Atlantic        Florida      21443.0          9587.0   21244317
# 43  West South Central          Texas      19199.0          6111.0   28628666
```
ex3
```python
# Sort homelessness by region, then descending family members
homelessness_reg_fam = homelessness.sort_values(["region", "family_members"], ascending=[True,False])

# Print the top few rows
print(homelessness_reg_fam)

#                 region      state  individuals  family_members  state_pop
# 13  East North Central   Illinois       6752.0          3891.0   12723071
# 35  East North Central       Ohio       6929.0          3320.0   11676341
# 22  East North Central   Michigan       5209.0          3142.0    9984072
# 49  East North Central  Wisconsin       2740.0          2167.0    5807406
# 14  East North Central    Indiana       3776.0          1482.0    6695497
```

### Subsetting columns
ex1
```python
# Select the individuals column
individuals = homelessness["individuals"]

# Print the head of the result
print(individuals.head())

#     0      2570.0
#     1      1434.0
#     2      7259.0
#     3      2280.0
#     4    109008.0
#     Name: individuals, dtype: float64
```

ex2
```python
# Select the state and family_members columns
state_fam = homelessness[["state", "family_members"]]

# Print the head of the result
print(state_fam.head())

#         state  family_members
# 0     Alabama           864.0
# 1      Alaska           582.0
# 2     Arizona          2606.0
# 3    Arkansas           432.0
# 4  California         20964.0
```

ex2

```python
# Select only the individuals and state columns, in that order
ind_state = homelessness[["individuals", "state"]]

# Print the head of the result
print(ind_state.head())

#        individuals       state
#     0       2570.0     Alabama
#     1       1434.0      Alaska
#     2       7259.0     Arizona
#     3       2280.0    Arkansas
#     4     109008.0  California
```

### Subsetting rows

ex1
```python
# Filter for rows where individuals is greater than 10000
ind_gt_10k = homelessness[homelessness["individuals"] > 10000]

# See the result
print(ind_gt_10k)

#                 region       state  individuals  family_members  state_pop
# 4              Pacific  California     109008.0         20964.0   39461588
# 9       South Atlantic     Florida      21443.0          9587.0   21244317
# 32        Mid-Atlantic    New York      39827.0         52070.0   19530351
# 37             Pacific      Oregon      11139.0          3337.0    4181886
# 43  West South Central       Texas      19199.0          6111.0   28628666
# 47             Pacific  Washington      16424.0          5880.0    7523869

```

ex2
```python
# Filter for rows where region is Mountain
mountain_reg = homelessness[homelessness["region"]=="Mountain"]

# See the result
print(mountain_reg)

#       region       state  individuals  family_members  state_pop
# 2   Mountain     Arizona       7259.0          2606.0    7158024
# 5   Mountain    Colorado       7607.0          3250.0    5691287
# 12  Mountain       Idaho       1297.0           715.0    1750536
# 26  Mountain     Montana        983.0           422.0    1060665
# 28  Mountain      Nevada       7058.0           486.0    3027341
# 31  Mountain  New Mexico       1949.0           602.0    2092741
# 44  Mountain        Utah       1904.0           972.0    3153550
# 50  Mountain     Wyoming        434.0           205.0     577601
```

ex3
```python
# Filter for rows where family_members is less than 1000 
# and region is Pacific
fam_lt_1k_pac = homelessness[ (homelessness["family_members"] < 1000) & (homelessness["region"]=="Pacific") ]

# See the result
print(fam_lt_1k_pac)

#     region   state  individuals  family_members  state_pop
# 1  Pacific  Alaska       1434.0           582.0     735139

```


### Subsetting rows by categorical variables

- ex1: 
Filter `homelessness` for cases where the USA census region is "South Atlantic" or it is "Mid-Atlantic", assigning to `south_mid_atlantic`. View the printed result.
```python
# Subset for rows in South Atlantic or Mid-Atlantic regions
south_mid_atlantic = homelessness[ homelessness["region"].isin(["South Atlantic", "Mid-Atlantic"]) ]

# See the result
print(south_mid_atlantic)

#             region                 state  individuals  family_members  state_pop
# 7   South Atlantic              Delaware        708.0           374.0     965479
# 8   South Atlantic  District of Columbia       3770.0          3134.0     701547
# 9   South Atlantic               Florida      21443.0          9587.0   21244317
# 10  South Atlantic               Georgia       6943.0          2556.0   10511131
# 20  South Atlantic              Maryland       4914.0          2230.0    6035802
# 30    Mid-Atlantic            New Jersey       6048.0          3350.0    8886025
# 32    Mid-Atlantic              New York      39827.0         52070.0   19530351
# 33  South Atlantic        North Carolina       6451.0          2817.0   10381615
# 38    Mid-Atlantic          Pennsylvania       8163.0          5349.0   12800922
# 40  South Atlantic        South Carolina       3082.0           851.0    5084156
# 46  South Atlantic              Virginia       3928.0          2047.0    8501286
# 48  South Atlantic         West Virginia       1021.0           222.0    1804291
```

- ex2: 
Filter `homelessness` for cases where the USA census `state` is in the list of Mojave states, `canu`, assigning to `mojave_homelessness`. View the printed result.
```python
# The Mojave Desert states
canu = ["California", "Arizona", "Nevada", "Utah"]

# Filter for rows in the Mojave Desert states
mojave_homelessness = homelessness[ homelessness["state"].isin(canu) ]

# See the result
print(mojave_homelessness)

#           region       state  individuals  family_members  state_pop
#     2   Mountain     Arizona       7259.0          2606.0    7158024
#     4    Pacific  California     109008.0         20964.0   39461588
#     28  Mountain      Nevada       7058.0           486.0    3027341
#     44  Mountain        Utah       1904.0           972.0    3153550
```

### --- New columns ---
![](https://i.imgur.com/k0M1d6g.png)
![](https://i.imgur.com/a7wf3CZ.png)



### Adding new columns

```python
# Add total col as sum of individuals and family_members
homelessness["total"] = homelessness["individuals"] + homelessness["family_members"]

# Add p_individuals col as proportion of total that are individuals
homelessness["p_individuals"] = homelessness["individuals"] / homelessness["total"]

# See the result
print(homelessness.head())

#                region       state  individuals  family_members  state_pop     total  p_individuals
# 0  East South Central     Alabama       2570.0           864.0    4887681    3434.0          0.748
# 1             Pacific      Alaska       1434.0           582.0     735139    2016.0          0.711
# 2            Mountain     Arizona       7259.0          2606.0    7158024    9865.0          0.736
# 3  West South Central    Arkansas       2280.0           432.0    3009733    2712.0          0.841
# 4             Pacific  California     109008.0         20964.0   39461588  129972.0          0.839
```

### Combo-attack!

```python
# Create indiv_per_10k col as homeless individuals per 10k state pop
homelessness["indiv_per_10k"] = 10000 * homelessness["individuals"] / homelessness["state_pop"] 

# Subset rows for indiv_per_10k greater than 20
high_homelessness = homelessness[ homelessness["indiv_per_10k"] > 20 ]

# Sort high_homelessness by descending indiv_per_10k
high_homelessness_srt = high_homelessness.sort_values("indiv_per_10k", ascending=False)

# From high_homelessness_srt, select the state and indiv_per_10k cols
result = high_homelessness_srt[["state", "indiv_per_10k"]]

# See the result
print(result)

#                    state  indiv_per_10k
# 8   District of Columbia         53.738
# 11                Hawaii         29.079
# 4             California         27.624
# 37                Oregon         26.636
# 28                Nevada         23.314
# 47            Washington         21.829
# 32              New York         20.392
```
---

## 2. Aggregating DataFrames
In this chapter, you’ll calculate summary statistics on DataFrame columns, and master grouped summary statistics and pivot tables.

### --- Summary statistics ---
![](https://i.imgur.com/YqZVhVH.png)
date min()
![](https://i.imgur.com/X2StwGc.png)
agg()
![](https://i.imgur.com/FBC3mgi.png)
![](https://i.imgur.com/9LNB0Sp.png)
![](https://i.imgur.com/rImppoZ.png)
cumsum
![](https://i.imgur.com/cKQe30O.png)
![](https://i.imgur.com/0EHeetC.png)

### Mean and median

```python
# Print the head of the sales DataFrame
print(sales.head())
#    store type  department       date  weekly_sales  is_holiday  temperature_c  fuel_price_usd_per_l  unemployment
# 0      1    A           1 2010-02-05      24924.50       False          5.728                 0.679         8.106
# 1      1    A           1 2010-03-05      21827.90       False          8.056                 0.693         8.106
# 2      1    A           1 2010-04-02      57258.43       False         16.817                 0.718         7.808
# 3      1    A           1 2010-05-07      17413.94       False         22.528                 0.749         7.808
# 4      1    A           1 2010-06-04      17558.09       False         27.050                 0.715         7.808
```
```python
# Print the info about the sales DataFrame
print(sales.info())

# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 10774 entries, 0 to 10773
# Data columns (total 9 columns):
#  #   Column                Non-Null Count  Dtype         
# ---  ------                --------------  -----         
#  0   store                 10774 non-null  int64         
#  1   type                  10774 non-null  object        
#  2   department            10774 non-null  int32         
#  3   date                  10774 non-null  datetime64[ns]
#  4   weekly_sales          10774 non-null  float64       
#  5   is_holiday            10774 non-null  bool          
#  6   temperature_c         10774 non-null  float64       
#  7   fuel_price_usd_per_l  10774 non-null  float64       
#  8   unemployment          10774 non-null  float64 
# dtypes: bool(1), datetime64[ns](1), float64(4), int32(1), int64(1), object(1)
# memory usage: 641.9+ KB
# None
```

```python
# Print the mean of weekly_sales
print(sales["weekly_sales"].mean())
# >>>23843.95014850566
# 
# Print the median of weekly_sales
print(sales["weekly_sales"].median())
# >>>12049.064999999999
```

### Summarizing dates

```python
# Print the maximum of the date column
print(sales["date"].max())

# Print the minimum of the date column
print(sales["date"].min())

# 2012-10-26 00:00:00
# 2010-02-05 00:00:00
```
### Efficient summaries
- ex1: Use the custom `iqr` function defined for you along with `.agg()` to print the IQR of the `temperature_c` column of `sales`.
```python
# A custom IQR function
def iqr(column):
    return column.quantile(0.75) - column.quantile(0.25)
    
# Print IQR of the temperature_c column
print(sales["temperature_c"].agg(iqr))
# >>> 16.583333333333336
```
- ex2: Update the column selection to use the custom `iqr` function with `.agg()` to print the IQR of `temperature_c, fuel_price_usd_per_l, and unemployment`, in that order.
```python
# A custom IQR function
def iqr(column):
    return column.quantile(0.75) - column.quantile(0.25)

# Update to print IQR of temperature_c, fuel_price_usd_per_l, & unemployment
print(sales[["temperature_c", "fuel_price_usd_per_l", "unemployment"]].agg(iqr))

# temperature_c           16.583
# fuel_price_usd_per_l     0.073
# unemployment             0.565
# dtype: float64
```
- ex3: Update the aggregation functions called by `.agg()`: include `iqr` and `np.median` in that order.
```python
# Import NumPy and create custom IQR function
import numpy as np
def iqr(column):
    return column.quantile(0.75) - column.quantile(0.25)

# Update to print IQR and median of temperature_c, fuel_price_usd_per_l, & unemployment
print(sales[["temperature_c", "fuel_price_usd_per_l", "unemployment"]].agg([iqr, np.median]))

#         temperature_c  fuel_price_usd_per_l  unemployment
# iqr            16.583                 0.073         0.565
# median         16.967                 0.743         8.099
```

### Cumulative statistics

```python
# Sort sales_1_1 by date
sales_1_1 = sales_1_1.sort_values("date")

# Get the cumulative sum of weekly_sales, add as cum_weekly_sales col
sales_1_1["cum_weekly_sales"] = sales_1_1["weekly_sales"].cumsum()

# Get the cumulative max of weekly_sales, add as cum_max_sales col
sales_1_1["cum_max_sales"] = sales_1_1["weekly_sales"].cummax()

# See the columns you calculated
print(sales_1_1[["date", "weekly_sales", "cum_weekly_sales", "cum_max_sales"]])

#             date  weekly_sales  cum_weekly_sales  cum_max_sales
# 0     2010-02-05      24924.50         2.492e+04       24924.50
# 1894  2010-02-05      21654.54         4.658e+04       24924.50
# 4271  2010-02-05     232558.51         2.791e+05      232558.51
# ...          ...           ...               ...            ...
# 6257  2012-10-12          3.00         2.569e+08      293966.05
# 3384  2012-10-26        -21.63         2.569e+08      293966.05

# [10774 rows x 4 columns]
```

### --- Counting ---
![](https://i.imgur.com/qnXFeq0.png)
drop duplicates
![](https://i.imgur.com/KcJDXPw.png)
![](https://i.imgur.com/T6YnPhW.png)
count
![](https://i.imgur.com/895XmWP.png)
proportions
![](https://i.imgur.com/GoSkn5l.png)


### Dropping duplicates


```python
# Drop duplicate store/type combinations
store_types = sales.drop_duplicates(subset=["store", "type"])
print(store_types.head())

# Drop duplicate store/department combinations
store_depts = sales.drop_duplicates(subset=["store", "department"])
print(store_depts.head())

# Subset the rows where is_holiday is True and drop duplicate dates
holiday_dates = sales[sales["is_holiday"]].drop_duplicates("date")

# Print date col of holiday_dates
print(holiday_dates["date"])
```

output
```
<script.py> output:
      store type  department       date  weekly_sales  is_holiday  temperature_c  fuel_price_usd_per_l  unemployment
0         1    A           1 2010-02-05      24924.50       False          5.728                 0.679         8.106
901       2    A           1 2010-02-05      35034.06       False          4.550                 0.679         8.324
1798      4    A           1 2010-02-05      38724.42       False          6.533                 0.686         8.623
2699      6    A           1 2010-02-05      25619.00       False          4.683                 0.679         7.259
3593     10    B           1 2010-02-05      40212.84       False         12.411                 0.782         9.765
---

    store type  department       date  weekly_sales  is_holiday  temperature_c  fuel_price_usd_per_l  unemployment
0       1    A           1 2010-02-05      24924.50       False          5.728                 0.679         8.106
12      1    A           2 2010-02-05      50605.27       False          5.728                 0.679         8.106
24      1    A           3 2010-02-05      13740.12       False          5.728                 0.679         8.106
36      1    A           4 2010-02-05      39954.04       False          5.728                 0.679         8.106
48      1    A           5 2010-02-05      32229.38       False          5.728                 0.679         8.106
---

498    2010-09-10
691    2011-11-25
2315   2010-02-12
6735   2012-09-07
6810   2010-12-31
6815   2012-02-10
6820   2011-09-09
Name: date, dtype: datetime64[ns]
```


### Counting categorical variables

```python
# Count the number of stores of each type
store_counts = store_types["type"].value_counts()
print(store_counts)

# Get the proportion of stores of each type
store_props = store_types["type"].value_counts(normalize=True)
print(store_props)

# Count the number of each department number and sort
dept_counts_sorted = store_depts["department"].value_counts(sort="department", ascending=False)
print(dept_counts_sorted)

# Get the proportion of departments of each number and sort
dept_props_sorted = store_depts["department"].value_counts(sort="department", normalize=True)
print(dept_props_sorted)
```

output
```
A    11
B     1
Name: type, dtype: int64
---
A    0.917
B    0.083
Name: type, dtype: float64
---
1     12
55    12
72    12
      ..
39     4
43     2
Name: department, Length: 80, dtype: int64
---
1     0.013
55    0.013
      ...  
39    0.004
43    0.002
Name: department, Length: 80, dtype: float64
```

### --- Grouped summary statistics ---
![](https://i.imgur.com/LYsON1Y.png)
不是個好方法 要複製多次 也不好debug
--> 用groupby
![](https://i.imgur.com/E5NC4Si.png)
更多用法
![](https://i.imgur.com/Ranql8V.png)
agg([min, max, np.mean, np.median])
![](https://i.imgur.com/DbBKUep.png)
![](https://i.imgur.com/BryvKLz.png)


### What percent of sales occurred at each store type?

```python
# Calc total weekly sales
sales_all = sales["weekly_sales"].sum()

# Subset for type A stores, calc total weekly sales
sales_A = sales[sales["type"] == "A"]["weekly_sales"].sum()

# Subset for type B stores, calc total weekly sales
sales_B = sales[sales["type"] == "B"]["weekly_sales"].sum()

# Subset for type C stores, calc total weekly sales
sales_C = sales[sales["type"] == "C"]["weekly_sales"].sum()

# Get proportion for each type
sales_propn_by_type = [sales_A, sales_B, sales_C] / sales_all
print(sales_propn_by_type)

# >>>[0.9097747 0.0902253 0.       ]
```

### Calculations with .groupby()
ex1
```python
# Group by type; calc total weekly sales
sales_by_type = sales.groupby("type")["weekly_sales"].sum()

# Get proportion for each type
sales_propn_by_type = sales_by_type / sum(sales_by_type)
print(sales_propn_by_type)


# type
# A    0.91
# B    0.09
# Name: weekly_sales, dtype: float64
```
ex2
```python
# From previous step
sales_by_type = sales.groupby("type")["weekly_sales"].sum()

# Group by type and is_holiday; calc total weekly sales
sales_by_type_is_holiday = sales.groupby(["type", "is_holiday"])["weekly_sales"].sum()
print(sales_by_type_is_holiday)

# <script.py> output:
# type  is_holiday
# A     False         2.337e+08
#       True          2.360e+04
# B     False         2.318e+07
#       True          1.621e+03
# Name: weekly_sales, dtype: float64
```

### Multiple grouped summaries

```python
# Import numpy with the alias np
import numpy as np

# For each store type, aggregate weekly_sales: get min, max, mean, and median
sales_stats = sales.groupby("type")["weekly_sales"].agg([min, max, np.mean, np.median])

# Print sales_stats
print(sales_stats)

# For each store type, aggregate unemployment and fuel_price_usd_per_l: get min, max, mean, and median
unemp_fuel_stats = sales.groupby("type")[["unemployment", "fuel_price_usd_per_l"]].agg([min, max, np.mean, np.median])

# Print unemp_fuel_stats
print(unemp_fuel_stats)


#          min        max       mean    median
# type                                        
# A    -1098.0  293966.05  23674.667  11943.92
# B     -798.0  232558.51  25696.678  13336.08
#      unemployment                      fuel_price_usd_per_l                     
#               min    max   mean median                  min    max   mean median
# type                                                                            
# A           3.879  8.992  7.973  8.067                0.664  1.107  0.745  0.735
# B           7.170  9.765  9.279  9.199                0.760  1.108  0.806  0.803
```

### --- Pivot tables ---
![](https://i.imgur.com/Bg4Vidq.png)
By default, pivot_table takes the **mean** value for each group.

![](https://i.imgur.com/LkM3D2U.png)
If we want a different summary statistic, we can use the **aggfunc** argument and pass it a function.

![](https://i.imgur.com/p0uTqL1.png)
To group by two variables, we can pass a second variable name into the **columns argument**.

![](https://i.imgur.com/vaFIt6Z.png)
NaN --> fill_value

![](https://i.imgur.com/lL7Vg30.png)
**margins**=True
The last row and last column of the pivot table contain the **mean** of all the values in the column or row, **not including the missing values** that were filled in with 0s. 

### Pivoting on one variable
ex1
```python
# Pivot for mean weekly_sales for each store type
mean_sales_by_type = sales.pivot_table(values="weekly_sales", index="type")

# Print mean_sales_by_type
print(mean_sales_by_type)

#       weekly_sales
# type              
# A        23674.667
# B        25696.678
```
ex2
```python
# Import NumPy as np
import numpy as np

# Pivot for mean and median weekly_sales for each store type
mean_med_sales_by_type = sales.pivot_table(values="weekly_sales", index="type", aggfunc=[np.mean, np.median])

# Print mean_med_sales_by_type
print(mean_med_sales_by_type)

#              mean       median
#      weekly_sales weekly_sales
# type                          
# A       23674.667     11943.92
# B       25696.678     13336.08
```
ex3
```python
# Pivot for mean weekly_sales by store type and holiday 
mean_sales_by_type_holiday = sales.pivot_table(values="weekly_sales", index="type", columns="is_holiday")

# Print mean_sales_by_type_holiday
print(mean_sales_by_type_holiday)

# is_holiday      False     True
# type                          
# A           23768.584  590.045
# B           25751.981  810.705
```


### Fill in missing values and sum values with pivot tables


```python
# Print mean weekly_sales by department and type; fill missing values with 0
print(sales.pivot_table(values="weekly_sales", index="department", columns="type", fill_value=0))

# type                 A           B
# department                        
# 1            30961.725   44050.627
# 2            67600.159  112958.527
# 3            17160.003   30580.655
# ...                ...         ...
# 98           12875.423     217.428
# 99             379.124       0.000

# [80 rows x 2 columns]
```

```python
# Print the mean weekly_sales by department and type; fill missing values with 0s; sum all rows and cols
print(sales.pivot_table(values="weekly_sales", index="department", columns="type", fill_value=0, margins=True))

# type                A           B        All
# department                                  
# 1           30961.725   44050.627  32052.467
# 2           67600.159  112958.527  71380.023
# 3           17160.003   30580.655  18278.391
# ...               ...         ...        ...
# 98          12875.423     217.428  11820.590
# 99            379.124       0.000    379.124
# All         23674.667   25696.678  23843.950

# [81 rows x 3 columns]
```

---

## 3. Slicing and Indexing DataFrames ---

### --- Explicit indexes ---
![](https://i.imgur.com/FfT5rm4.png)
![](https://i.imgur.com/H0fH3Fd.png)
![](https://i.imgur.com/qa1rqnc.png)
![](https://i.imgur.com/8Jm8xU6.png)
![](https://i.imgur.com/WS62nPO.png)
`reset_index` has a drop argument that allows you to discard an index. Here, setting drop to True entirely removes the dog names.

![](https://i.imgur.com/uyIvSu9.png)
You may be wondering why you should bother with indexes. The answer is that **it makes subsetting code cleaner**. 
DataFrames have a subsetting method called "**loc**" which filters on index values. 

![](https://i.imgur.com/Kfop17i.png)
Index values **don't need to be unique**
The values in the index don't need to be unique. Here, there are two Labradors in the index.

![](https://i.imgur.com/MyMFJod.png)
You can include multiple columns in the index by passing a list of column names to set_index.
![](https://i.imgur.com/oDsNNfg.png)
![](https://i.imgur.com/RKEH5kd.png)

![](https://i.imgur.com/Fe1zJ4a.png)
sort_index()
By default, it sorts all index levels from outer to inner, in ascending order.

![](https://i.imgur.com/HQ73lKf.png)
ascending

![](https://i.imgur.com/lAtxVe7.png)
Now you have two problems
Indexes are controversial. Although they simplify subsetting code, there are some downsides. 
- Index values are just data. Storing data in multiple forms makes it harder to think about. There is a concept called "tidy data," where data is stored in tabular form - like a DataFrame. Each row contains a single observation, and each variable is stored in its own column. 
- Indexes violate the last rule since index values don't get their own column. In pandas, the syntax for working with indexes is different from the syntax for working with columns. By using two syntaxes, your code is more complicated, which can result in more bugs. 
- If you decide you don't want to use indexes, that's perfectly reasonable. However, it's useful to know how they work for cases when you need to read other people's code.

![](https://i.imgur.com/8Q72tST.png)

### Setting and removing indexes

```python
# Look at temperatures
print(temperatures.head())

# Index temperatures by city
temperatures_ind = temperatures.set_index("city")

# Look at temperatures_ind
print(temperatures_ind.head())

# Reset the index, keeping its contents
print(temperatures_ind.reset_index())

# Reset the index, dropping its contents
print(temperatures_ind.reset_index(drop=True))
```
output
```
<script.py> output:
            date     city        country  avg_temp_c
    0 2000-01-01  Abidjan  Côte D'Ivoire      27.293
    1 2000-02-01  Abidjan  Côte D'Ivoire      27.685
    2 2000-03-01  Abidjan  Côte D'Ivoire      29.061
    3 2000-04-01  Abidjan  Côte D'Ivoire      28.162
    4 2000-05-01  Abidjan  Côte D'Ivoire      27.547
    ---
                  date        country  avg_temp_c
    city                                         
    Abidjan 2000-01-01  Côte D'Ivoire      27.293
    Abidjan 2000-02-01  Côte D'Ivoire      27.685
    Abidjan 2000-03-01  Côte D'Ivoire      29.061
    Abidjan 2000-04-01  Côte D'Ivoire      28.162
    Abidjan 2000-05-01  Côte D'Ivoire      27.547
    ---
              city       date        country  avg_temp_c
    0      Abidjan 2000-01-01  Côte D'Ivoire      27.293
    1      Abidjan 2000-02-01  Côte D'Ivoire      27.685
    2      Abidjan 2000-03-01  Côte D'Ivoire      29.061
    ...        ...        ...            ...         ...
    16497     Xian 2013-07-01          China      25.251
    16498     Xian 2013-08-01          China      24.528
    16499     Xian 2013-09-01          China         NaN
    
    [16500 rows x 4 columns]
    ---
                date        country  avg_temp_c
    0     2000-01-01  Côte D'Ivoire      27.293
    1     2000-02-01  Côte D'Ivoire      27.685
    2     2000-03-01  Côte D'Ivoire      29.061
    ...          ...            ...         ...
    16497 2013-07-01          China      25.251
    16498 2013-08-01          China      24.528
    16499 2013-09-01          China         NaN
    
    [16500 rows x 3 columns]
```

### Subsetting with .loc[]

```python
# Make a list of cities to subset on
cities = ["Moscow", "Saint Petersburg"]

# Subset temperatures using square brackets
print(temperatures[temperatures["city"].isin(cities)])

# Subset temperatures_ind using .loc[]
print(temperatures_ind.loc[cities])
```
output
```
            date              city country  avg_temp_c
10725 2000-01-01            Moscow  Russia      -7.313
10726 2000-02-01            Moscow  Russia      -3.551
10727 2000-03-01            Moscow  Russia      -1.661
...          ...               ...     ...         ...
13362 2013-07-01  Saint Petersburg  Russia      17.234
13363 2013-08-01  Saint Petersburg  Russia      17.153
13364 2013-09-01  Saint Petersburg  Russia         NaN

[330 rows x 4 columns]
---
                       date country  avg_temp_c
city                                           
Moscow           2000-01-01  Russia      -7.313
Moscow           2000-02-01  Russia      -3.551
Moscow           2000-03-01  Russia      -1.661
...                     ...     ...         ...
Saint Petersburg 2013-07-01  Russia      17.234
Saint Petersburg 2013-08-01  Russia      17.153
Saint Petersburg 2013-09-01  Russia         NaN

[330 rows x 3 columns]
```

### Setting multi-level indexes

```python
# Index temperatures by country & city
temperatures_ind = temperatures.set_index(["country", "city"])

# List of tuples: Brazil, Rio De Janeiro & Pakistan, Lahore
rows_to_keep = [("Brazil", "Rio De Janeiro"), ("Pakistan", "Lahore")]

# Subset for rows to keep
print(temperatures_ind.loc[rows_to_keep])
```
output
```
                              date  avg_temp_c
country  city                                 
Brazil   Rio De Janeiro 2000-01-01      25.974
         Rio De Janeiro 2000-02-01      26.699
         Rio De Janeiro 2000-03-01      26.270
...                            ...         ...
Pakistan Lahore         2013-05-01      33.457
         Lahore         2013-06-01      34.456
         Lahore         2013-07-01      33.279
         Lahore         2013-08-01      31.511
         Lahore         2013-09-01         NaN

[330 rows x 2 columns]
```

### Sorting by index values

```python
# Sort temperatures_ind by index values
print(temperatures_ind.sort_index())

# Sort temperatures_ind by index values at the city level
print(temperatures_ind.sort_index(level=["city","country"]))

# Sort temperatures_ind by country then descending city
print(temperatures_ind.sort_index(level=["country", "city"], ascending=[True, False]))
```

output
```
                         date  avg_temp_c
country     city                         
Afghanistan Kabul  2000-01-01       3.326
            Kabul  2000-02-01       3.454
            Kabul  2000-03-01       9.612
...                       ...         ...
Zimbabwe    Harare 2013-05-01      18.298
            Harare 2013-06-01      17.020
            Harare 2013-07-01      16.299
...                       ...         ...

[16500 rows x 2 columns]
---
                            date  avg_temp_c
country       city                          
Côte D'Ivoire Abidjan 2000-01-01      27.293
              Abidjan 2000-02-01      27.685
              Abidjan 2000-03-01      29.061
...                          ...         ...
China         Xian    2013-05-01      18.979
              Xian    2013-06-01      23.522
              Xian    2013-07-01      25.251
...                       ...         ...

[16500 rows x 2 columns]
---
                         date  avg_temp_c
country     city                         
Afghanistan Kabul  2000-01-01       3.326
            Kabul  2000-02-01       3.454
            Kabul  2000-03-01       9.612
...                       ...         ...
Zimbabwe    Harare 2013-05-01      18.298
            Harare 2013-06-01      17.020
            Harare 2013-07-01      16.299
...                       ...         ...

[16500 rows x 2 columns]
```

### --- Slicing and subsetting with .loc and .iloc ---
![](https://i.imgur.com/VKTFjDl.png)
![](https://i.imgur.com/MlQqV46.png)
![](https://i.imgur.com/UfapKa5.png)
![](https://i.imgur.com/R6V4fYe.png)
The same technique doesn't work on **inner index** levels. 
It's important to understand the **danger** here. pandas doesn't throw an error to let you know that there is a problem, so be careful when coding.
![](https://i.imgur.com/6zxKmyo.png)
正確用法

![](https://i.imgur.com/alipaML.png)
![](https://i.imgur.com/8wGpeEL.png)

![](https://i.imgur.com/PTvqJEO.png)
date
![](https://i.imgur.com/qWtxJaz.png)


### Slicing index values

```python
# Sort the index of temperatures_ind
temperatures_srt = temperatures_ind.sort_index()

# Subset rows from Pakistan to Russia
print(temperatures_srt.loc["Pakistan":"Russia"])

# Try to subset rows from Lahore to Moscow
print(temperatures_srt.loc["Lahore":"Moscow"])

# Subset rows from Pakistan, Lahore to Russia, Moscow
print(temperatures_srt.loc[("Pakistan","Lahore"):("Russia","Moscow")])
```
output
```

                                date  avg_temp_c
country  city                                   
Pakistan Faisalabad       2000-01-01      12.792
         Faisalabad       2000-02-01      14.339
...                              ...         ...
Russia   Saint Petersburg 2013-05-01      12.355
         Saint Petersburg 2013-06-01      17.185
...                              ...         ...
[1155 rows x 2 columns]
---
                         date  avg_temp_c
country city                             
Mexico  Mexico     2000-01-01      12.694
        Mexico     2000-02-01      14.677
...                       ...         ...
Morocco Casablanca 2013-05-01      19.217
        Casablanca 2013-06-01      23.649
...                              ...         ...
[330 rows x 2 columns]
---
                      date  avg_temp_c
country  city                         
Pakistan Lahore 2000-01-01      12.792
         Lahore 2000-02-01      14.339
...                    ...         ...
Russia   Moscow 2013-05-01      16.152
         Moscow 2013-06-01      18.718
...                              ...         ...
[660 rows x 2 columns]
```

### Slicing in both directions

```python
# Subset rows from India, Hyderabad to Iraq, Baghdad
print(temperatures_srt.loc[("India","Hyderabad"):("Iraq","Baghdad")])

# Subset columns from date to avg_temp_c
print(temperatures_srt.loc[:,"date":"avg_temp_c"])

# Subset in both directions at once
print(temperatures_srt.loc[("India","Hyderabad"):("Iraq","Baghdad"),"date":"avg_temp_c"])
```
output
```
                        date  avg_temp_c
country city                            
India   Hyderabad 2000-01-01      23.779
        Hyderabad 2000-02-01      25.826
...                      ...         ...
Iraq    Baghdad   2013-05-01      28.673
        Baghdad   2013-06-01      33.803
...                       ...         ...
[2145 rows x 2 columns]
---

                         date  avg_temp_c
country     city                         
Afghanistan Kabul  2000-01-01       3.326
            Kabul  2000-02-01       3.454
...                       ...         ...
Zimbabwe    Harare 2013-05-01      18.298
            Harare 2013-06-01      17.020
...                       ...         ...
[16500 rows x 2 columns]
---
                        date  avg_temp_c
country city                            
India   Hyderabad 2000-01-01      23.779
        Hyderabad 2000-02-01      25.826
...                      ...         ...
Iraq    Baghdad   2013-05-01      28.673
        Baghdad   2013-06-01      33.803
...                       ...         ...
[2145 rows x 2 columns]

```

### Slicing time series

```python
# Use Boolean conditions to subset temperatures for rows in 2010 and 2011
temperatures_bool = temperatures[(temperatures["date"] >= "2010-01-01") & (temperatures["date"] <= "2011-12-31")]
print(temperatures_bool)

# Set date as an index and sort the index
temperatures_ind = temperatures.set_index("date").sort_index()

# Use .loc[] to subset temperatures_ind for rows in 2010 and 2011
print(temperatures_ind.loc["2010":"2011"])

# Use .loc[] to subset temperatures_ind for rows from Aug 2010 to Feb 2011
print(temperatures_ind.loc["2010-08":"2011-02"])
```
output
```

            date     city        country  avg_temp_c
120   2010-01-01  Abidjan  Côte D'Ivoire      28.270
121   2010-02-01  Abidjan  Côte D'Ivoire      29.262
...                ...        ...         ...
16474 2011-08-01     Xian          China      23.069
16475 2011-09-01     Xian          China      16.775
...                ...        ...         ...
[2400 rows x 4 columns]
---
                  city    country  avg_temp_c
date                                         
2010-01-01  Faisalabad   Pakistan      11.810
2010-01-01   Melbourne  Australia      20.016
...                ...        ...         ...
2011-12-01      Nagoya      Japan       6.476
2011-12-01   Hyderabad      India      23.613
...                ...        ...         ...
[2400 rows x 3 columns]
---
                city        country  avg_temp_c
date                                           
2010-08-01  Calcutta          India      30.226
2010-08-01      Pune          India      24.941
...                ...        ...         ...
2011-02-01     Kabul    Afghanistan       3.914
2011-02-01   Chicago  United States       0.276
...                ...        ...         ...
[700 rows x 3 columns]
```

### Subsetting by row/column number

```python
# Get 23rd row, 2nd column (index 22, 1)
print(temperatures.iloc[22,1])

# Use slicing to get the first 5 rows
print(temperatures.iloc[:5])

# Use slicing to get columns 3 to 4
print(temperatures.iloc[:,2:4])

# Use slicing in both directions at once
print(temperatures.iloc[:5,2:4])
```
output
```
Abidjan
---

        date     city        country  avg_temp_c
0 2000-01-01  Abidjan  Côte D'Ivoire      27.293
1 2000-02-01  Abidjan  Côte D'Ivoire      27.685
2 2000-03-01  Abidjan  Côte D'Ivoire      29.061
3 2000-04-01  Abidjan  Côte D'Ivoire      28.162
4 2000-05-01  Abidjan  Côte D'Ivoire      27.547
---
             country  avg_temp_c
0      Côte D'Ivoire      27.293
1      Côte D'Ivoire      27.685
...              ...         ...
16498          China      24.528
16499          China         NaN

[16500 rows x 2 columns]
---

         country  avg_temp_c
0  Côte D'Ivoire      27.293
1  Côte D'Ivoire      27.685
2  Côte D'Ivoire      29.061
3  Côte D'Ivoire      28.162
4  Côte D'Ivoire      27.547
```

### --- Working with pivot tables ---
![](https://i.imgur.com/egeNDHF.png)
![](https://i.imgur.com/wLIhzfm.png)
![](https://i.imgur.com/7vsdRcI.png)
![](https://i.imgur.com/OQBLlsY.png)



### Pivot temperature by city and year

```python
# Add a year column to temperatures
temperatures["year"] = temperatures["date"].dt.year

# Pivot avg_temp_c by country and city vs year
temp_by_country_city_vs_year = temperatures.pivot_table(values="avg_temp_c", index=["country","city"], columns="year")

# See the result
print(temp_by_country_city_vs_year)
```
output
```
year                              2000    2001    2002    2003    2004  ...    2009    2010    2011    2012    2013
country       city                                                      ...                                        
Afghanistan   Kabul             15.823  15.848  15.715  15.133  16.128  ...  15.093  15.676  15.812  14.510  16.206
Angola        Luanda            24.410  24.427  24.791  24.867  24.216  ...  24.325  24.440  24.151  24.240  24.554
...                                ...     ...     ...     ...     ...  ...     ...     ...     ...     ...     ...
United States Chicago           11.090  11.703  11.532  10.482  10.943  ...  10.298  11.816  11.214  12.821  11.587
              Los Angeles       16.643  16.466  16.430  16.945  16.553  ...  16.677  15.887  15.875  17.090  18.121
              New York           9.969  10.931  11.252   9.836  10.389  ...  10.142  11.358  11.272  11.971  12.164
Vietnam       Ho Chi Minh City  27.589  27.832  28.065  27.828  27.687  ...  27.853  28.282  27.675  28.249  28.455
Zimbabwe      Harare            20.284  20.861  21.079  20.889  20.308  ...  20.524  21.166  20.782  20.523  19.756

[100 rows x 14 columns]
```

### Subsetting pivot tables

```python
# Subset for Egypt to India
temp_by_country_city_vs_year.loc["Egypt":"India"]

# Subset for Egypt, Cairo to India, Delhi
temp_by_country_city_vs_year.loc[("Egypt","Cairo"):("India","Delhi")]

# Subset in both directions at once
temp_by_country_city_vs_year.loc[("Egypt","Cairo"):("India","Delhi"),"2005":"2010"]
```

output
```
year                    2005    2006    2007    2008    2009    2010
country  city                                                       
Egypt    Cairo        22.006  22.050  22.361  22.644  22.625  23.718
         Gizeh        22.006  22.050  22.361  22.644  22.625  23.718
Ethiopia Addis Abeba  18.313  18.427  18.143  18.165  18.765  18.298
France   Paris        11.553  11.788  11.751  11.278  11.464  10.410
Germany  Berlin        9.919  10.545  10.883  10.658  10.062   8.607
India    Ahmadabad    26.828  27.283  27.511  27.049  28.096  28.018
         Bangalore    25.477  25.418  25.464  25.353  25.726  25.705
         Bombay       27.036  27.382  27.635  27.178  27.845  27.765
         Calcutta     26.729  26.986  26.585  26.522  27.153  27.289
         Delhi        25.716  26.366  26.146  25.675  26.554  26.520
```

### Calculating on a pivot table

```python
# Get the worldwide mean temp by year
mean_temp_by_year = temp_by_country_city_vs_year.mean(axis="index")

# Filter for the year that had the highest mean temp
print(mean_temp_by_year[mean_temp_by_year == max(mean_temp_by_year)])

# Get the mean temp by city
mean_temp_by_city = temp_by_country_city_vs_year.mean(axis="columns")

# Filter for the city that had the lowest mean temp
print(mean_temp_by_city[mean_temp_by_city == min(mean_temp_by_city)])
```

output
```
year
2013    20.312
dtype: float64
country  city  
China    Harbin    4.877
dtype: float64
```


## 4. Creating and Visualizing DataFrames

### --- Visualizing your data ---
![](https://i.imgur.com/ChQeOc8.png)
![](https://i.imgur.com/oXiBQNn.png)
bin

**kind**
![](https://i.imgur.com/3H0xhtQ.png)
bar plot
![](https://i.imgur.com/viRz54o.png)
line plot
![](https://i.imgur.com/NUw94Q7.png)
rot
![](https://i.imgur.com/oA6vWxk.png)
scatter plot
![](https://i.imgur.com/FSezxUr.png)
layer plot + legend + Transparency

![](https://i.imgur.com/oK7I3SM.png)

### Which avocado size is most popular?

```python
# Import matplotlib.pyplot with alias plt
import matplotlib.pyplot as plt

# Look at the first few rows of data
print(avocados.head())

#          date          type  year  avg_price   size    nb_sold
# 0  2015-12-27  conventional  2015       0.95  small  9.627e+06
# 1  2015-12-20  conventional  2015       0.98  small  8.710e+06
# 2  2015-12-13  conventional  2015       0.93  small  9.855e+06
# 3  2015-12-06  conventional  2015       0.89  small  9.405e+06
# 4  2015-11-29  conventional  2015       0.99  small  8.095e+06

# Get the total number of avocados sold of each size
nb_sold_by_size = avocados.groupby("size")["nb_sold"].sum()

# Create a bar plot of the number of avocados sold by size
nb_sold_by_size.plot(kind="bar")

# Show the plot
plt.show()
```
![](https://i.imgur.com/ZAhdkbT.png)


### Changes in sales over time

```python
# Import matplotlib.pyplot with alias plt
import matplotlib.pyplot as plt

# Get the total number of avocados sold on each date
nb_sold_by_date = avocados.groupby("date")["nb_sold"].sum()

# Create a line plot of the number of avocados sold by date
nb_sold_by_date.plot(kind="line")

# Show the plot
plt.show()
```
![](https://i.imgur.com/a51mxVp.png)




### Avocado supply and demand
```python
# Scatter plot of avg_price vs. nb_sold with title
avocados.plot(x="nb_sold", y="avg_price", kind="scatter", title="Number of avocados sold vs. average price")

# Show the plot
plt.show()
```

![](https://i.imgur.com/DizaYuz.png)


### Price of conventional vs. organic avocados
```python
# Histogram of conventional avg_price 
avocados[avocados["type"] == "conventional"]["avg_price"].hist(alpha=0.5, bins=20)

# Histogram of organic avg_price
avocados[avocados["type"] == "organic"]["avg_price"].hist(alpha=0.5, bins=20)

# Add a legend
plt.legend(["conventional", "organic"])

# Show the plot
plt.show()
```
![](https://i.imgur.com/rzPbfOb.png)



### --- Missing values ---
![](https://i.imgur.com/cjP2UBz.png)
In a pandas DataFrame, missing values are indicated with **N-a-N**, which stands for "not a number."
![](https://i.imgur.com/N7Mg7wk.png)
- `.isna()`

![](https://i.imgur.com/Eq7VrTB.png)
- `isna().any()`
we get one value for each variable that tells us if there are any missing values in that column.

![](https://i.imgur.com/VVJhG7v.png)

![](https://i.imgur.com/ldgcAm3.png)

![](https://i.imgur.com/8N7zeyJ.png)
- `.dropna()` Removing missing values
![](https://i.imgur.com/IWcXsqU.png)
- `.fillna(0)` Replacing missing values



### Finding missing values

```python
# Import matplotlib.pyplot with alias plt
import matplotlib.pyplot as plt

# Check individual values for missing values
print(avocados_2016.isna())

# Check each column for missing values
print(avocados_2016.isna().any())

# Bar plot of missing values by variable
avocados_2016.isna().sum().plot(kind="bar")

# Show plot
plt.show()
```
output
```
     date  avg_price  total_sold  small_sold  large_sold  xl_sold  total_bags_sold  small_bags_sold  large_bags_sold  xl_bags_sold
0   False      False       False       False       False    False            False            False            False         False
1   False      False       False       False       False    False            False            False            False         False
2   False      False       False       False        True    False            False            False            False         False
...
50  False      False       False        True       False    False            False            False            False         False
51  False      False       False        True       False    False            False            False            False         False

---
date               False
avg_price          False
total_sold         False
small_sold          True
large_sold          True
xl_sold             True
total_bags_sold    False
small_bags_sold    False
large_bags_sold    False
xl_bags_sold       False
dtype: bool
```
![](https://i.imgur.com/EKOFsgZ.png)


### Removing missing values

```python
# Remove rows with missing values
avocados_complete = avocados_2016.dropna()

# Check if any columns contain missing values
print(avocados_complete.isna().any())
```
output
```
date               False
avg_price          False
total_sold         False
small_sold         False
large_sold         False
xl_sold            False
total_bags_sold    False
small_bags_sold    False
large_bags_sold    False
xl_bags_sold       False
dtype: bool
```

### Replacing missing values
ex1
```python
# List the columns with missing values
cols_with_missing = ["small_sold", "large_sold", "xl_sold"]

# Create histograms showing the distributions cols_with_missing
avocados_2016[cols_with_missing].plot(kind="hist")

# Show the plot
plt.show()
```
![](https://i.imgur.com/VxVo4mi.png)

ex2
```python
# From previous step
cols_with_missing = ["small_sold", "large_sold", "xl_sold"]
#avocados_2016[cols_with_missing].hist()
#plt.show()

# Fill in missing values with 0
avocados_filled = avocados_2016.fillna(0)

# Create histograms of the filled columns
avocados_filled[cols_with_missing].hist()

# Show the plot
plt.show()
```
![](https://i.imgur.com/cD8oN1L.png)

### --- Creating DataFrames ---
![](https://i.imgur.com/NexGDn1.png)
![](https://i.imgur.com/OllC4a5.png)
![](https://i.imgur.com/WDoQE8i.png)
![](https://i.imgur.com/CNRcmAC.png)


### List of dictionaries

```python
# Create a list of dictionaries with new data
avocados_list = [
    {"date": "2019-11-03", "small_sold": 10376832, "large_sold": 7835071},
    {"date": "2019-11-10", "small_sold": 10717154, "large_sold": 8561348},
]

# Convert list into DataFrame
avocados_2019 = pd.DataFrame(avocados_list)

# Print the new DataFrame
print(avocados_2019)
```
output
```
         date  small_sold  large_sold
0  2019-11-03    10376832     7835071
1  2019-11-10    10717154     8561348
```

### Dictionary of lists

```python
# Create a dictionary of lists with new data
avocados_dict = {
  "date": ["2019-11-17","2019-12-01"],
  "small_sold": [10859987,9291631],
  "large_sold": [7674135,6238096]
}

# Convert dictionary into DataFrame
avocados_2019 = pd.DataFrame(avocados_dict)

# Print the new DataFrame
print(avocados_2019)
```
output
```

         date  small_sold  large_sold
0  2019-11-17    10859987     7674135
1  2019-12-01     9291631     6238096
```

### --- Reading and writing CSVs ---
![](https://i.imgur.com/ngm1lMv.png)
![](https://i.imgur.com/pJbdOZC.png)
![](https://i.imgur.com/qjFqq8z.png)
![](https://i.imgur.com/XwJoIiq.png)
![](https://i.imgur.com/SiwOfme.png)

### CSV to DataFrame
- ex1
```python
# Read CSV as DataFrame called airline_bumping
airline_bumping = pd.read_csv("airline_bumping.csv")

# Take a look at the DataFrame
print(airline_bumping.head())
```
output
```
             airline  year  nb_bumped  total_passengers
0    DELTA AIR LINES  2017        679          99796155
1     VIRGIN AMERICA  2017        165           6090029
2    JETBLUE AIRWAYS  2017       1475          27255038
3    UNITED AIRLINES  2017       2067          70030765
4  HAWAIIAN AIRLINES  2017         92           8422734
```
- ex2
```python
# For each airline, select nb_bumped and total_passengers and sum
airline_totals = airline_bumping.groupby("airline")[["nb_bumped", "total_passengers"]].sum()
```
output
```
             airline  year  nb_bumped  total_passengers
0    DELTA AIR LINES  2017        679          99796155
1     VIRGIN AMERICA  2017        165           6090029
2    JETBLUE AIRWAYS  2017       1475          27255038
3    UNITED AIRLINES  2017       2067          70030765
4  HAWAIIAN AIRLINES  2017         92           8422734
```
- ex3
```python
# Create new col, bumps_per_10k: no. of bumps per 10k passengers for each airline
airline_totals["bumps_per_10k"] = airline_totals["nb_bumped"]/ airline_totals["total_passengers"] * 10000
# Print airline_totals
print(airline_totals)
```
output
```
                     nb_bumped  total_passengers  bumps_per_10k
airline                                                        
ALASKA AIRLINES           1392          36543121          0.381
AMERICAN AIRLINES        11115         197365225          0.563
DELTA AIR LINES           1591         197033215          0.081
EXPRESSJET AIRLINES       3326          27858678          1.194
FRONTIER AIRLINES         1228          22954995          0.535
HAWAIIAN AIRLINES          122          16577572          0.074
JETBLUE AIRWAYS           3615          53245866          0.679
SKYWEST AIRLINES          3094          47091737          0.657
SOUTHWEST AIRLINES       18585         228142036          0.815
SPIRIT AIRLINES           2920          32304571          0.904
UNITED AIRLINES           4941         134468897          0.367
VIRGIN AMERICA             242          12017967          0.201
```

### DataFrame to CSV

```python
# Create airline_totals_sorted
airline_totals_sorted = airline_totals.sort_values("bumps_per_10k", ascending=False)

# Print airline_totals_sorted
print(airline_totals_sorted)

# Save as airline_totals_sorted.csv
airline_totals_sorted.to_csv("airline_totals_sorted.csv")
```

output
```
                     nb_bumped  total_passengers  bumps_per_10k
airline                                                        
EXPRESSJET AIRLINES       3326          27858678          1.194
SPIRIT AIRLINES           2920          32304571          0.904
SOUTHWEST AIRLINES       18585         228142036          0.815
JETBLUE AIRWAYS           3615          53245866          0.679
SKYWEST AIRLINES          3094          47091737          0.657
AMERICAN AIRLINES        11115         197365225          0.563
FRONTIER AIRLINES         1228          22954995          0.535
ALASKA AIRLINES           1392          36543121          0.381
UNITED AIRLINES           4941         134468897          0.367
VIRGIN AMERICA             242          12017967          0.201
DELTA AIR LINES           1591         197033215          0.081
HAWAIIAN AIRLINES          122          16577572          0.074
```

### --- Wrap-up ---
![](https://i.imgur.com/CH49zqU.png)
![](https://i.imgur.com/aMJV2wu.png)

--- Congratulations! ---
