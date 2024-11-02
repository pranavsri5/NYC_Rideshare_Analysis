Project Tasks and Methodology
Task 1: Data Loading and Preprocessing
Load Data: Loaded rideshare_data.csv and taxi_zone_lookup.csv into Spark DataFrames using read.csv.
Join Data: Merged datasets by renaming columns and performing joins based on pickup and dropoff locations.
Date Conversion: Converted UNIX timestamps to yyyy-mm-dd format using from_unixtime and date_format.
Task 2: Monthly Trip Counts, Profits, and Driver Earnings
Trip Counts: Extracted month from the date column, grouped by business and month, and calculated monthly trip counts.
Monthly Profits: Converted profit to float, grouped by month, and calculated total monthly profits.
Driver Earnings: Converted driver_total_pay to float, grouped by month, and calculated monthly earnings.
Task 3: Top-K Processing
Top Pickup/Dropoff Locations: Identified top 5 popular pickup and dropoff boroughs monthly using window functions and ranking.
Top Routes: Found top 30 profitable routes by grouping routes and calculating total profits.
Task 4: Driver Earnings Analysis
Analyzed average driver pay, trip length, and earnings per mile by time of day, highlighting the highest earnings periods for profitability insights.

Task 5: Waiting Time Analysis
Average Waiting Time: Calculated average waiting time in January by day.
Insights: Visualized waiting time trends, showing high demand on specific days (e.g., New Year's Day).
Task 6: Trip Counts by Pickup Borough and Time of Day
Trip Counts: Filtered data for trips in specified ranges and calculated trip counts.
Borough-Specific Analysis: Analyzed trips from Brooklyn to Staten Island.
Key APIs and Functions Used
Data Loading: read.csv(), withColumnRename()
Joins and Aggregations: join(), groupby(), agg(), count()
Date and Time: from_unixtime(), date_format()
Window Functions: window(), partitionby(), rank()
Filtering and Sorting: filter(), orderby()
Insights and Strategic Decision-Making
This project provides insights on:

Demand patterns by month, useful for driver and company strategies.
Profit and earnings trends to optimize resource allocation.
Popular routes and peak times for profitability improvements.
Challenges and Solutions
Schema Management: Used inferSchema=True to ensure data types are correctly identified.
Complex Joins: Renamed columns to avoid conflicts during joins.
Aggregation Complexity: Managed using groupby() and window() functions.
