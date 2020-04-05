# Data Processing Using PySpark
The project contains the scripts to process data, residing inside the Resources directory, using PySpark.
# Prerequisite
Spark should be setup on the machine. To get the latest version of spark 
[click here](https://spark.apache.org/downloads.html "spark.apache.org")
# Getting Started
Following instructions will get you a copy of the project up and running on your local machine for development and testing 
purposes.

1. Clone the repository using below command:\
   ```git clone <https://github.com/iftikhar1995/PySpark.git>```

2. Following are the steps to **setup PySpark in PyCharm**:
   1. Open the cloned project in PyCharm.
   2. On the menu bar navigate to `File > Settings > Project > Project > Structure`.
   3. Locate `+Add Content Root` and click on it.
   4. Add the path of **pyspark**  from **spark** directory e.g `~/spark/python/`.
   5. Save the configuration.
 
 3. Following are the steps to run spark job, locally.
    1. Open a script.
    2. Right click on it.
    3. In the Menu, click `Run <script_name>.py` to execute spark job. 
 
 # Project Structure
 Below is the project structure:
 ```
    PySpark
        |- RDD
        |   |-<data>DataAnalysis
        |   |        |- scripts(*.py)
        |
        |- Resources
        |   |-<data>
        |        |- data files (*.txt| *.csv etc.)
```

- **PySpark :** The main folder. It contains all the scripts and resources.
  - **RDD :** The folder contains all spark scripts which are using RDDs
    - **\<data\>DataAnalysis :** The folder contains the spark scripts that are performing analysis on the `_\<data\>_`.
       The `_\<data\>_` is just a placeholder, which will be replaced by `folder name` mentioned in `Resources` folder.
      - **scripts(\*.py) :** The spark script which is performing analysis on the data provided in `Resources` folder.
  - **Resources :** The folder contains the data on which we are doing analysis.
    - **\<data\> :** The folders containing the data files
      - **data files (\*.txt| \*.csv etc.) :** The actual data files                                