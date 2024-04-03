# Assignment Tasks

## Task 1: DataFetch Pipeline

### Overview:
The DataFetch Pipeline is designed to fetch data files from a specific URL, randomly select a specified number of files, download them, zip them into an archive, and move the archive to a target location.

### Sub Tasks:
- **Fetch Data**
  - Fetch the page containing the location wise datasets for that year. (Bash Operator with wget or curl command)command.
  - Stores the fetched page as an HTML file.
- **Select Files**
  - select_rand_files
  - Based on the required number of data files, select the data files randomly from the available list of files
- **Downlad Files**
  - file_download, fetch_ind_files
  - Fetch the individual data files
- **Zip Files**
  - zip_files
  - Zip them into an archive.
- **Move Archive**
  - move_archive_loc
  - Place the archive at a required location.

### DAG Configuration:
- DAG ID: ncei_data_fetch
- Owner: admin
- description: A data fetch pipeline
- Start Date: 2024-01-01
- Retries: 1

## Task 2: Analytic Pipeline

### Overview:
The Analytic Pipeline performs data analytics and visualization on the fetched data files. It waits for the archive to be available, unzips the archive, extracts CSV contents, filters data, computes monthly averages, and creates visualizations using geopandas.

### Sub Tasks:
- **Wait For Archive**
  - Wait for the archive to be available (with a timeout of 5 secs) at the destined location. 
  - If the wait has timed out, stop the pipeline.
- **Unzip Archive**
  - Upon the availability (status=success), check if the file is a valid archive followed by unzipthe contents into individual CSV files.
- **Process CSV Files**
  - Extract the contents of the CSV into a data frame and filter the dataframe based on the required fields such Windspeed or BulbTemperature, etc. Extract also the Lat/Long values from the CSV to create a tuple of the form <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>.
- **Compute Monthly Averages**
  - Compute the monthly averages of the required fields.
  - The output will be of the form <Lat, Long, [[Avg_11, ..., Avg_1N] .. [Avg_M1, ..., Avg_MN]]> for N fields and M months.
- **Create Visualization**
  - Create a visualization where you plot the required fields (one per field) using heatmaps at different lat/lon positions.
  - Export the plots to PNG.
- **Delete CSV File**
  - Upon successful completion, delete the CSV file from the destined location.

### DAG Configuration:
- DAG ID: analytics_pipeline
- Owner: airflow
- Description: Analytics pipeline for data visualization
- Start Date: 2024-01-01
- Retries: 1
- 'retry_delay': 5 minutes
