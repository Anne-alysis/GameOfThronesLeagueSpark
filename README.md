# GameOfThronesLeagueSpark
Spark version of https://github.com/Anne-alysis/GameOfThronesLeague (without plotting).

Note 1) this will not be able to be run without modifications for your particular cluster and storage system.  
This assumes the Spark job will be run remotely on a Dataproc cluster using Google Cloud Project (via the non-scrubbed 
version of `game-of-thrones-spark/etc/run-github.sh`) using Google Cloud Storage (GCS) buckets for storage.    

Note 2) file names and locations below are for reference only, in the `resources` folder.  Actual files 
for running the jar are all stored in and written to GCS buckets.  

# Introduction

In honor of the final season of Game of Thrones, we put together a fantasy league!  Each participant fills
out a form we developed before the start of the season. Week by week, possible answers to each question are updated, and 
are scored by an algorithm. I developed the scoring algorithm that is in this repo. The form itself is accessible by Google 
forms and is not included here. Questions and current correct answers can be viewed in the file, `game-of-thrones-spark/src/main/resources/correct_answers.xlsx`. 

Example questions include:
 
 * Whether a character lives or dies
 * Who rides a dragon
 * Who ends up on the Iron Throne (if it still exists)

At the end of the season, the top ranked team wins a prize and the Iron Throne. 


# General Code Structure

This code will be run weekly and scores recalculated based on new information in each week's episode.  Week by 
week the correct answers will be updated, and responses re-evaluated against changing information (e.g., 
a character dies).  The path for files below is `game-of-thrones-spark/src/main/resources/` and are given for context 
only.  All files to run the jar are stored and accessed from GCS.  

1) The code reads in the responses from a downloaded CSV (`fantasy_game_of_thrones_responses.csv`)
2) Responses are reshaped to allow for ease of scoring
3) Answers from a Excel sheet are read in (`correct_answers.xlsx`)
4) Scores are computed and unaggregated results written (`raw_results.csv`)
5) Scores are written to a CSV (`results.csv`), including the previous weeks' scores/ranks/rank movement, if applicable.  

Note, the algorithm was tested with a mock answer sheet as well (`correct_answer_testing.xlsx`).

# Compiling and running the code

The code is compiled using Maven: `mvn -pl game-of-thrones-spark package`.  The compiled jar is pushed to GCS via the below
shell script so it can be run on a Dataproc cluster.  

Each week an argument is read in to indicate the value of the week (e.g., 1 - 6). The `week` variable must be changed via the shell script.  
After replacing the scrubbed variables in the script, it can be run via:

`sh game-of-thrones-spark/etc/run-github.sh`

# Classes
## `Score.scala`

This is the main class which includes all steps outlined above.  It includes a companion object that stores the filenames and reads in the arguments from the shell script: storage bucket (`bucket`) and week (`week`).  

The class (not companion object) extends a trait in `common/Utilities.scala` that creates the Spark session, which is then passed implicitly to all other methods that require a Spark session.  

## `common` package

This is a shared package that includes classes to start up a Spark session (`SparkUtils`, `DefaultSparkSession`), neither of which were authored by Anne-alysis.  

### `Utilities.scala`

A custom DataFrame melt method is stored here that transforms wide data into long data.  In addition, a `Spark` trait is also in this mixed class that creates the Spark session as the implicit variable `spark`.  


## `io` package
### `InputHandling.scala`

This object handles all the input operations, such as:
 * Reading in responses and reshaping that data 
    * If `week == 1`, the raw data will be reshaped and written to a file.  For subsequent weeks,
    this data will be read in from this file, instead of being reshaped each time, which is inefficient and unnecessary.
 * Creating the structure of the question sheet, for first week only.  
 
 ### `OutputHandling.scala`

This object handles all the output operations, such as:
* Combining previous weeks' results (if `week > 1`)
 * Writing out the results
 
 ## `calculations` package
### `Calculations.scala`
This object includes the methods for scoring.  Most are exact match algorithms, but
some questions have multiple possible answers.  Both cases are handled in a UDF.  Grouping the final
results by team also happen in this object.  



