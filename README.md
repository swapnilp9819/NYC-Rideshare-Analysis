# NYC Rideshare Analysis

## Overview
This GitHub repository contains the project files for the NYC Rideshare Analysis, where we applied Apache Spark to analyze a comprehensive dataset of Uber and Lyft rides from January 1, 2023, to May 31, 2023. The analysis focuses on uncovering insights about ride frequencies, driver earnings, passenger waiting times, and route popularity to inform operational strategies for rideshare companies.

## Dataset
The analysis utilizes two primary datasets provided by the NYC Taxi and Limousine Commission (TLC), distributed under the MIT license:
1. `rideshare_data.csv` - Contains detailed information on individual rideshare trips.
2. `taxi_zone_lookup.csv` - Provides mapping information for taxi zones mentioned in the rideshare dataset.

### Dataset Accessibility
- **Source and Licensing**: The datasets are sourced from the NYC Taxi and Limousine Commission (TLC) Trip Record Data, available under the MIT license. More details and data can be accessed [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

### Schema Overview
#### rideshare_data.csv
- **business:** Uber or Lyft
- **pickup_location:** Taxi zone ID where the trip started
- **dropoff_location:** Taxi zone ID where the trip ended
- **trip_length:** Distance in miles
- **request_to_pickup:** Time from ride request to pickup in seconds
- **total_ride_time:** Total ride duration in seconds
- **date:** Date of the ride in UNIX timestamp

#### taxi_zone_lookup.csv
- **LocationID:** Numeric ID corresponding to taxi zones
- **Borough:** NYC borough
- **Zone:** Specific area within the borough
- **service_zone:** Service area category

## Project Structure
This project consists of several analytical tasks implemented using Spark:
- **Task 1: Merging Datasets** - Integrating rideshare data with taxi zone information.
- **Task 2: Aggregation of Data** - Analyzing trip counts, platform profits, and driver earnings.
- **Task 3: Top-K Processing** - Identifying top pickup and dropoff boroughs and busiest routes.
- **Task 4: Average of Data** - Calculating average earnings, trip lengths, and earnings per mile by time of day.
- **Task 5: Finding Anomalies** - Examining anomalies in waiting times during January.
- **Task 6: Filtering Data** - Filtering data to find specific trip count ranges and routes between boroughs.
- **Task 7: Routes Analysis** - Determining the most popular routes based on total trip counts.

## Project File Structure

The project is structured into separate scripts and Jupyter notebooks for each analytical task. Below is a detailed breakdown of the files corresponding to each task:

### Task 1: Merging Datasets
- **Merging_Datasets_01.py**: Script for merging the rideshare and taxi zone lookup datasets. **This script must be run first as it prepares the data necessary for all subsequent analyses.**

### Task 2: Aggregation of Data
- **Aggregation_of_Data_02.py**: Script for aggregating data to calculate trip counts, platform profits, and driver earnings.
- **Aggregation_of_Data_Visualisation_02.ipynb**: Jupyter notebook used for visualizing the aggregated data.

### Task 3: Top-K Processing
- **Top_K_Processing_03.py**: Script for identifying the top pickup and dropoff boroughs and the busiest routes.

### Task 4: Average of Data
- **Average_of_Data_04.py**: Script for calculating average earnings, trip lengths, and earnings per mile by time of day.

### Task 5: Finding Anomalies
- **Finding_anomalies_05.py**: Script for detecting anomalies in waiting times during January.
- **Finding_anomalies_Visualisation_05.ipynb**: Jupyter notebook used for visualizing the anomalies in waiting times.

### Task 6: Filtering Data
- **Filtering_Data_06.py**: Script for filtering data to find specific trip count ranges and routes between boroughs.

### Task 7: Routes Analysis
- **Routes_Analysis_07.py**: Script for analyzing the most popular routes based on total trip counts.

## Methods and APIs Used

In this project, several Apache Spark functions and additional APIs were utilized to manipulate data, perform analyses, and prepare outputs for visualization. Below are key methods and their applications across different tasks:

### Data Manipulation and Analysis
- **Spark SQL Functions**: Used for filtering, aggregating, and transforming

 datasets. Functions such as `groupBy`, `agg`, `sum`, `avg`, and `orderBy` were instrumental in performing complex data manipulations.
- **DataFrame Operations**: Including `filter`, `select`, `withColumn`, `drop`, used extensively to prepare and adjust data for analysis.

### Data Exporting
- **Export to CSV**: After processing the data with Spark, the results were exported to CSV files for visualization and sharing. This was achieved using:
  - `ccc method bucket ls`: Command to list the contents of our S3 bucket, ensuring we targeted the correct dataset for export.
  - `ccc method bucket cp -r bkt:task-specific-folder output_folder`: Command to copy processed data from our S3 bucket to a designated output location, making it accessible for further analysis in tools like Jupyter Notebook.

### Visualization
- **Jupyter Notebook**: Used for generating histograms and other visual representations of the data. After exporting the data to CSV, Jupyter Notebooks were employed to visualize trends and anomalies using libraries like Matplotlib and Seaborn.

### API Integrations
- **S3 Bucket Integration**: Integrated AWS S3 buckets for secure and scalable storage of raw and processed data, which was essential for handling large datasets efficiently.

## Useful Resources
- [PySpark by Examples](https://sparkbyexamples.com/pyspark)
- [PySpark Tutorial](https://sparkbyexamples.com/pyspark-tutorial/)
- [Apache Spark Python API Docs](https://spark.apache.org/docs/3.1.2/api/python/getting_started/index.html)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Key Findings
- **Market Dominance:** Uber consistently outperforms Lyft in trip volume and profitability, especially in Manhattan.
- **Earnings Insights:** Drivers earn more in the afternoon and on longer trips at night, suggesting optimal times for drivers to work.
- **Anomaly Detection:** Notable increase in waiting times on New Year’s Day, likely due to increased demand.
- **Route Popularity:** Specific routes like Brooklyn to Staten Island show unexpectedly high traffic, indicating areas for operational focus.

## Conclusion
The analysis provided in this repository illustrates the power of big data in understanding and optimizing the rideshare industry. Insights gained from this project can help rideshare companies refine their strategies, improve driver satisfaction, and enhance customer service.

## How to Use
To replicate this analysis:
1. Clone this repository.
2. Ensure Apache Spark and required libraries are installed.
3. Execute the Spark scripts provided in the `scripts` folder.
