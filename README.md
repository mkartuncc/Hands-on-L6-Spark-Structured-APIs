# Music Streaming Analysis Using Spark Structured APIs
## Overview
This repository is designed to demonstrate the use of simple Spark Structured APIs using a small dataset. 

## Dataset Description
This repository will contain two datasets with the following fields and schemas, created by running the datagen.py file: 

1. songs_metadata.csv- song_id, title, artist, genre, mood


         |-- root
         
          |-- user_id: string (nullable = true)
       
          |-- song_id: string (nullable = true)
       
          |-- timestamp: timestamp (nullable = true)
       
          |-- duration_sec: integer (nullable = true)
 
2. listening_logs.csv- user_id, song_id, timestamp, duration_sec

   
        |-- root
         
          |-- song_id: string (nullable = true)
         
          |-- title: string (nullable = true)
         
          |-- artist: string (nullable = true)
         
          |-- genre: string (nullable = true)
         
          |-- mood: string (nullable = true)


## Repository Structure
      |-- Hands-on-L6-Spark-Structured-APIs
      
         |-- datagen.py
         
         |-- main.py
         
         |-- README.md
      
         |-- Requirements



## Output Directory Structure
After running the datagen.py file:

> python datagen.py

The two datasets will be created as songs_metadata.csv and listening_logs.csv. The output directory will be structured as below: 

    |-- Hands-on-L6-Spark-Structured-APIs
      
         |-- datagen.py
         
         |-- main.py
         
         |-- README.md
      
         |-- Requirements
         
         |-- songs_metadata.csv
         
         |-- listening_logs.csv

## Tasks and Outputs

### Task 1: User Favorite Genres (top 10)

#### Output: 

+-------+---------+-----+

|user_id|    genre|count|

+-------+---------+-----+

|user_12|     Jazz|    8|

|user_72|Classical|    8|

|user_32|Classical|    6|

|user_81|     Jazz|    6|

|user_64|     Jazz|    6|

|user_69|Classical|    6|

| user_8|     Jazz|    6|

|user_82|Classical|    6|

|user_87|Classical|    6|

|user_99|     Rock|    6|

+-------+---------+-----+

### Task 2: Average Listen Time (top 10) 

#### Output: 

+-------+------------------+

|user_id|  avg_duration_sec|

+-------+------------------+

|user_24|             254.0|

|user_70|236.77777777777777|

|user_16|             225.0|

|user_49|219.11111111111111|

|user_17|210.27272727272728|

|user_85|209.92307692307693|

| user_4|           209.375|

|user_75|             207.0|

|user_43|206.33333333333334|

|user_58|205.45454545454547|

+-------+------------------+

### Task 3: Genre Loyalty Scores (total listen time per genre per user, ranked, top 10)

#### Output: 

+-------+---------+-------------+

|user_id|    genre|loyalty_score|

+-------+---------+-------------+

|user_12|     Jazz|         1366|

|user_99|     Rock|         1289|

|user_69|Classical|         1232|

|user_72|  Hip-Hop|         1218|

|user_32|Classical|         1149|

|user_98|  Hip-Hop|         1146|

|user_72|Classical|         1141|

|user_74|     Rock|         1104|

|user_85|      Pop|         1056|

|user_37|     Rock|          988|

+-------+---------+-------------+

### Task 4: Identify users who listen between 12 AM and 5 AM (all)

#### Output: 

+--------+

|user_id |

+--------+

|user_58 |

|user_94 |

|user_14 |

|user_56 |

|user_22 |

|user_68 |

|user_86 |

|user_97 |

|user_65 |

|user_47 |

|user_66 |

|user_21 |

|user_19 |

|user_18 |

|user_13 |

|user_51 |

|user_7  |

|user_95 |

|user_52 |

|user_5  |

|user_9  |

|user_20 |

|user_83 |

|user_71 |

|user_64 |

|user_93 |

|user_72 |

|user_4  |

|user_48 |

|user_1  |

|user_10 |

|user_28 |

|user_12 |

|user_32 |

|user_62 |

|user_82 |

|user_53 |

|user_43 |

|user_50 |

|user_40 |

|user_41 |

|user_11 |

|user_59 |

|user_35 |

|user_90 |

|user_99 |

|user_27 |

|user_98 |

|user_44 |

|user_96 |

|user_17 |

|user_26 |

|user_38 |

|user_16 |

|user_100|

|user_42 |

|user_15 |

|user_91 |

|user_34 |

|user_8  |

|user_88 |

|user_92 |

|user_60 |

|user_78 |

|user_6  |

|user_76 |

|user_54 |

|user_87 |

|user_2  |

|user_55 |

|user_24 |

|user_69 |

|user_37 |

|user_84 |

|user_89 |

|user_74 |

|user_3  |

|user_61 |

|user_29 |

|user_70 |

|user_36 |

|user_23 |

|user_63 |

|user_77 |

|user_30 |

+--------+


## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 datagen.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions

Using a virtual environment for this repository is strongly recommended. The following commands were used to run the final code after installing the prerequisites:

> python datagen.py

> python main.py
