# %% [markdown]
# Big Data Lab - Assignment 2
# Sreerag S (CE20B111)

# %% [markdown]
# ## Task 1 (DataFetch Pipeline)

# %%
# Importing modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from bs4 import BeautifulSoup
from datetime import datetime
import random
import shutil
import urllib
import os

# %%
url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
year = 2005
n_files = 2
archive_directory = '/tmp/archive' 

# %%
# The settings specified in the run_dag configuration are immutable and should be modified exclusively at this location.
csv_file_directory = '/tmp/data/' + '{{params.year}}/' # Provide a designated location to save CSV data files if needed; otherwise, utilize a temporary location.
Webpage_directory = '/tmp/webpage/' # Specify a location to store HTML files if necessary, otherwise, resort to a temporary location.

# %%
# Creating a default conf dictionary
conf = {
    'url' :url,
    'year' :Param(year, type="integer", minimum=1901,maximum=2024),
    'n_files' : n_files,
    'archive_directory' :archive_directory,  
}

# %%
# Define DAG properties

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Create an instance of a DAG
dag1 = DAG(
    dag_id = "ncei_data_fetch",
    default_args=default_args,
    params = conf,
    description='A data fetch pipeline',
    schedule=None,
)


# %% [markdown]
# ### Tasks

# %% [markdown]
# 1.Fetch the page containing the location wise datasets for that year. (Bash Operator with wget or curl command)

# %%
# Create a dictionary to hold parameters for fetching the page
fetch_params = dict(
    url = "{{params.url}}",
    file_directory = Webpage_directory
    )

# Create a BashOperator task to retrieve the page using wget
fetch_page_task = BashOperator(
    task_id=f"download_html_data",
    bash_command="curl {{params.url}}{{params.year}}/ --create-dirs -o {{params.file_directory}}{{params.year}}.html",
    params = fetch_params,
    dag=dag1,
)

# %% [markdown]
# fetch_page_task.run() to run the fetch code

# %% [markdown]
# 2. Based on the required number of data files, select the data files randomly from the available list of files. (Python Operator)

# %%
def parse_page_content(page_data, url, year):
    """
    Extracts CSV file URLs from HTML page data for a specific year.

    Args:
        page_data (str): The HTML content of the page.
        url (str): The base URL of the page.
        year (int): The year for which CSV file URLs are to be extracted.

    Returns:
        list: A list of CSV file URLs.
    """
    # Initialize an empty list to store the extracted CSV file URLs
    csv_urls = []

    # Create the URL for the specified year
    year_url = f"{url}/{year}/"

    # Parse the HTML page content using BeautifulSoup
    soup = BeautifulSoup(page_data, 'html.parser')

    # Find all anchor tags (hyperlinks) in the HTML page
    links = soup.find_all('a')

    # Iterate through each hyperlink and get the 'href' attribute of the hyperlink
    for link in links:
        
        href = link.get('href')

        # Check if the href contains ".csv", indicating a CSV file
        if ".csv" in href:
            # Create the full URL of the CSV file
            csv_url = f'{year_url}{href}'
            
            # Add the CSV file URL to the result list
            csv_urls.append(csv_url)

    return csv_urls

# %%
def select_rand_files(n_files, url_base, y_params,file_dir,**kwargs):
    """
    Selects a specified number of random files from a list of available files.

    Args:
        n_files (int): Number of files to select randomly.
        url_base (str): The base URL for file URLs.
        y_params (str): Year parameters for selecting the HTML file.
        file_dir (str): The directory containing the HTML file.

    Returns:
        list: A list of randomly selected file URLs.
    """
    # Get the HTML file content
    filename = f"{file_dir}{y_params}.html"
    # Read the content of the HTML file
    with open(filename, "r") as f:
        pages_content = f.read()
    # Parse the HTML page content to extract the list of available files
    files_available = parse_page_content(pages_content,url=url_base,year=y_params)

    # Select a random sample of files from the available files
    select_file_url = random.sample(files_available, int(n_files))
    
    return select_file_url


# Define a dictionary to store parameters for selecting files
select_fil_params = dict(
    n_files = "{{params.n_files}}",
    y_params = "{{params.year}}",
    url_base = "{{params.url}}",
    file_dir = Webpage_directory
)
# Define Select files task
select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_rand_files,
    op_kwargs=select_fil_params,
    dag=dag1,
)

# %% [markdown]
# select_files_task.run() to run the task

#%%
# 3. Fetch the individual data files (Bash or Python Operator)

# %%
def file_download(file_url, csv_output_dir):
    # Create the output directory if it doesn't exist
    os.makedirs(csv_output_dir, exist_ok=True)
    # Extract the file name from the URL and decode any URL-encoded characters
    file_name = urllib.parse.unquote(os.path.basename(file_url))
    # Construct the full path where the file is saved
    file_path = os.path.join(csv_output_dir, file_name)
    # Use the curl command to download the file from the given URL and save it to the specified path
    os.system(f"curl {file_url} -o {file_path}")
    # Return the name of the downloaded file
    return file_name


# %%
def fetch_ind_files(csv_output_dir,**kwargs):
    ti = kwargs['ti']
    selected_files = ti.xcom_pull(task_ids='select_files')
    for file_url in selected_files:
        file_download(file_url, csv_output_dir)

# %%

# Defining parameters to pass
fetch_param = dict( csv_output_dir = csv_file_directory )

# Task to download CSV
fetch_tasks = PythonOperator(
    task_id='fetch_files',
    python_callable=fetch_ind_files,
    op_kwargs=fetch_param,
    dag=dag1,
)

# %% [markdown]
# 4. Zip them into an archive. (Python Operator)

# %%
# Zip the files
def zip_files(output_dir, archive_path, **kwargs):
    shutil.make_archive(archive_path, 'zip', output_dir)

#%% 
archive_path = csv_file_directory[:-1] if csv_file_directory[-1]=='/' else csv_file_directory
#Params for zip_files function
zip_params = {
    'output_dir': csv_file_directory,
    'archive_path' : archive_path}

# Creating Task
zip_tasks = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    op_kwargs=zip_params,
    dag=dag1,
)

# %% [markdown]
# 5. Place the archive at a required location. (Bash/Python Operator)

# %%
# Move the archive to the required location
def move_archive_loc(archive_path, target_location, **kwargs):
    os.makedirs(target_location, exist_ok=True)
    shutil.move(archive_path + '.zip', os.path.join(target_location , str(kwargs['dag_run'].conf.get('year'))) + '.zip')

# %%
# move_archive_loc function params
move_archive_loc_params = dict(
    target_location = "{{params.archive_directory}}",
    archive_path = archive_path
    )

# Creating Task
move_archive_task = PythonOperator(
    task_id='move_archive',
    python_callable=move_archive_loc,
    op_kwargs=move_archive_loc_params,
    dag=dag1,
)

# %%
# Define task dependencies
fetch_page_task >> select_files_task >> fetch_tasks >> zip_tasks >> move_archive_task




# %%
#------------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------
# Task 2 (analytic pipeline)

from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from geodatasets import get_path
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
import numpy as np
import logging
import apache_beam as beam
import matplotlib.pyplot as plt
import shutil
import os
from ast import literal_eval

# Path to archive file
archive_path = "/tmp/archive/2005.zip"
required_fields = "WindSpeed, BulbTemperature"

conf = dict(
    archive_path = archive_path,
    required_fields = required_fields
)

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create an instance of a DAG
dag = DAG(
    dag_id='analytics_pipeline',
    default_args=default_args,
    description='Analytics pipeline for data visualization',
    params = conf,
    schedule_interval='*/1 * * * *',
    catchup=False,
)

# 1. Wait for the archive to be available (with a timeout of 5 secs) at the destined location. If the wait has timed out, stop the pipeline. (FileSensor)
wait_archive_task = FileSensor(
    task_id = 'wait_for_archive',
    mode="poke",
    poke_interval = 5,  
    timeout = 5,
    filepath = "{{params.archive_path}}",
    dag=dag,
    fs_conn_id = "my_file_connect",
)

# 2. Upon the availability (status=success), check if the file is a valid archive followed by unzipthe contents into individual CSV files. (BashOperator)
unzip_archive_task = BashOperator(
    task_id='unzip_archive',
    bash_command="unzip -o {{params.archive_path}} -d /tmp/data2",
    dag=dag,
)



# 3. Extract the contents of the CSV into a data frame and filter the dataframe based on the required fields such Windspeed or BulbTemperature, etc. Extract also the Lat/Long values from the CSV to create a tuple of the form <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>. (PythonOperator using Apache Beam)
def parse_csv(data):
        # Parse fields from the CSV
        df = data.split('","')
        df[0] = df[0].strip('"')
        df[-1] = df[-1].strip('"')
        return list(df)

def extract_filter_fields(element, required_fields):
    headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
    headers = {header: index for index, header in enumerate(headers_csv)}
    lat = element[headers['LATITUDE']]
    lon = element[headers['LONGITUDE']]
    arr = []
    for ind,i in enumerate(headers_csv):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        arr.append(ind)
    data = [element[index] for index in arr if index < len(element)]
    if lat != 'LATITUDE':
        yield ((lat, lon), data)



def process_csv_files(required_fields):
    required_fields = [field.strip() for field in required_fields.split(",")]
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline() as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/tmp/data2/*.csv')
            | 'ParseData' >> beam.Map(parse_csv)
            | 'ExtractAndFilterFields' >> beam.ParDo(extract_filter_fields, required_fields)
            | 'GroupBy' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a: (a[0][0], a[0][1], a[1]))  # Unpack the tuple
            | 'WriteToFile' >> beam.io.WriteToText('/tmp/results/result.txt')
        )


required_fields = dict(
    required_fields = "{{ params.required_fields }}",
)

process_csv_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv_files,
    op_kwargs = required_fields,
    dag=dag,
)


# 4. Setup another PythonOperator over ApacheBeam to compute the monthly averages of the required fields. The output will be of the form <Lat, Long, [[Avg_11, ..., Avg_1N] .. [Avg_M1, ..., Avg_MN]]> for N fields and M months.
def extract_fields_with_month(element, required_fields):
    headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
    headers = {header: index for index, header in enumerate(headers_csv)}
    lat = element[headers['LATITUDE']]
    lon = element[headers['LONGITUDE']]
    arr = []
    for ind,i in enumerate(headers_csv):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        arr.append(ind)
    data = [element[index] for index in arr if index < len(element)]
    if lat != 'LATITUDE':
        measure_time = datetime.strptime(element[headers['DATE']], '%Y-%m-%dT%H:%M:%S')
        month_format = "%Y-%m"
        month = measure_time.strftime(month_format)
        yield ((month, lat, lon), data)
        
def averages(data):
    # Function to compute monthly averages
    val_data = np.array(data[1])
    val_data_shape = val_data.shape
    val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') # converting to float removing empty string and replace with nan
    val_data = np.reshape(val_data,val_data_shape)
    masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
    res = np.ma.average(masked_data, axis=0)
    res = list(res.filled(np.nan))
    logger = logging.getLogger(__name__)
    logger.info(res)
    return ((data[0][1],data[0][2]),res)

def monthly_averages(required_fields, **kwargs):
    # Logic to compute monthly averages using Apache Beam
    required_fields = list(map(lambda a:a.strip(),required_fields.split(",")))
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        # Apache Beam pipeline code
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/tmp/data2/*.csv')
            | 'ParseData' >> beam.Map(parse_csv)
            | 'CreateTupleWithMonthInKey' >> beam.ParDo(extract_fields_with_month,required_fields)
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: averages(data))
            | 'CombineTuplewithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        # save result to a file
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/tmp/results/averages.txt')
        

compute_monthly_average_task = PythonOperator(
    task_id='monthly_averages',
    python_callable=monthly_averages,
    op_kwargs = required_fields,
    dag=dag,
)
# %%
class Aggregate(beam.CombineFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i in headers_csv:
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(i.replace('Hourly',''))

    def accumulators(self):
        return []
    
    def add_input(self, accumulator, element):
        accumulator_ = {key:value for key,value in accumulator}
        # Extract data from element
        data = element[2]
        val_data = np.array(data)
        val_data_shape = val_data.shape
        # Convert data to numeric, replacing empty strings with NaN
        val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') 
        val_data = np.reshape(val_data,val_data_shape)
        # Mask NaN values in data
        masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
        avg_values  = np.ma.average(masked_data, axis=0)
        # Fill masked values with NaN and convert to list
        filled_avg_values = list(avg_values.filled(np.nan))
        for ind,i in enumerate(self.required_fields):
            accumulator_[i] = accumulator_.get(i,[]) + [(element[0],element[1],filled_avg_values[ind])]

        return list(accumulator_.items())
    
    def accumulator_merge(self, accumulators):
        merged = {}
        for a in accumulators:
                d = {key:value for key,value in a}
                for i in self.required_fields:
                    merged[i] = merged.get(i,[]) + d.get(i,[])

        return list(merged.items())
    
    def extract_output(self, accumulator):
        return accumulator

def plot_geomaps(values):
    logger = logging.getLogger(__name__)
    logger.info(values)
    arr = []
    for i in range(len(values[1])):
        if values[1][i][0] == '':
            arr.append(i)
    for i in arr[::-1]:
        del values[1][i]

    d1 = np.array(values[1],dtype='float')
    data = np.array(values[1], dtype='float')
    d1 = np.array(data, dtype='float')
    # Load a sample Map of world to plot
    world = gpd.read_file(get_path('naturalearth.land'))

    # Create a GeoDataFrame with some random data for demonstration
    data = gpd.GeoDataFrame({
        values[0]: d1[:, 2]
    }, geometry=gpd.points_from_xy(*d1[:, (1, 0)].T))
    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    world.plot(ax=ax, color='white', edgecolor='black')
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=150, ax=ax, legend=True)
    ax.set_title(f'{values[0]} Heatmap')
    os.makedirs('/tmp/results/plots', exist_ok=True)
    # Save the plot to PNG
    plt.savefig(f'/tmp/results/plots/{values[0]}_heatmap_plot.png')

# 5. create a visualization where you plot the required fields (one per field) using heatmaps at different lat/lon positions. Export the plots to PNG. (PythonOperator using ApacheBeam)
def create_heatmap_visual(required_fields, **kwargs):
    required_fields = list(map(lambda a: a.strip(), required_fields.split(",")))
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'Read Processed Data' >> beam.io.ReadFromText('/tmp/results/averages.txt*')
            | 'PreprocessParse' >> beam.Map(lambda a: literal_eval(a.replace('nan', 'None')))
            | 'Globalaggregation' >> beam.CombineGlobally(Aggregate(required_fields = required_fields))
            | 'Flatten Results' >> beam.FlatMap(lambda a: a)
            | 'Plot Geomaps' >> beam.Map(plot_geomaps)
        )


create_heatmap_visual_task = PythonOperator(
    task_id='create_heatmap_visualization',
    python_callable=create_heatmap_visual,
    op_kwargs = required_fields,
    dag=dag,
)

# 7. Upon successful completion, delete the CSV file from the destined location.
def csv_file_delete(**kwargs):
    shutil.rmtree('/tmp/data2')

delete_csv_file_task = PythonOperator(
    task_id='delete_csv_file',
    python_callable=csv_file_delete,
    dag=dag,
)

# Set task dependencies
wait_archive_task >> unzip_archive_task >> process_csv_files_task
process_csv_files_task >> delete_csv_file_task
unzip_archive_task >> compute_monthly_average_task
compute_monthly_average_task >> create_heatmap_visual_task
create_heatmap_visual_task >> delete_csv_file_task
