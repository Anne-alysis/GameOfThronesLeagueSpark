# GameOfThronesLeagueSpark
Spark version of https://github.com/Anne-alysis/GameOfThronesLeague (without plotting).

Note: this will not be able to be run without modifications for your particular cluster and storage system.  This assumes the Spark job will be run remotely on a Dataproc cluster in Google Cloud (via the non-scrubbed version of `game-of-thrones-spark/etc/run-github.sh`) using GCS buckets for storage.  


# Introduction

In honor of the final season of Game of Thrones, we put together a fantasy league!  Each participant fills
out a form we developed before the start of the season. Week by week, possible answers to each question are updated, and 
are scored by an algorithm. I developed the scoring algorithm that is in this repo. The form itself is accessible by Google 
forms and is not included here. Questions and current correct answers can be viewed in the file, `./external-files/answer_truth.xlsx`. 

Example questions include:
 
 * Whether a character lives or dies
 * Who rides a dragon
 * Who ends up on the Iron Throne (if it still exists)

At the end of the season, the top ranked team wins a prize and the Iron Throne. 


# General Code Structure

This code will be run weekly and scores recalculated based on new information in each week's episode.  Week by 
week the correct answers will be updated, and responses re-evaluated against changing information (e.g., 
a character dies).

1) The code reads in the responses from a downloaded CSV (`./external-files/Fantasy Game of Thrones Responses.csv`)
2) Responses are reshaped to allow for ease of scoring
3) Answers from a Excel sheet are read in (`./external-files/answer_truth.xlsx`)
4) Scores are computed 
5) Scores are written to a CSV (`./external-files/Results.csv`), including the previous weeks' scores/ranks/rank movement, if applicable.  

Note, the algorithm was tested with a mock answer sheet as well (`./external-files/answer_testing.xlsx`).

# Running the code

Each week an argument is read in to indicate the value of the week (e.g., 1 - 6). The `week` variable must be changed via the shell script.  After replacing the scrubbed variables in the script, it can be run via:

`sh game-of-thrones-spark/etc/run-github.sh`

There is also a flag to be set in the shell script, `createquestionstructure`, that allows the structure of the questions to be written to a file for ease of building the correct answer sheet.

# Classes
## `Score.scala`

This is the main class which includes all steps outlined above.  It includes a companion object that stores the filenames and reads in the arguments from the shell script: storage bucket (`bucket`), week (`week`), and the boolean question structure flag (`createQuestionStructure`).  

The class (not companion object) extends a trait in `common/Utilities.scala` that creates the Spark session, which is then passed implicitly to all other methods that require a Spark session.  

## `common` package

This is a shared package that includes classes to start up a Spark session (`SparkUtils`, `DefaultSparkSession`), neither of which were authored by Anne-alysis.  

### `Utilities.scala`

A custom DataFrame melt method is stored here that transforms wide data into long data.  In addition, a `Spark` trait is also in this mixed class that creates the Spark session as the implicit variable `spark`.  


## `io` package
### `InputHandling.scala`

This object handles all the input operations, such as:
 * Reading in responses and reshaping that data
 * Creating the structure of the question sheet
 
 ### `OutputHandling.scala`

This object handles all the output operations, such as:
* Combining previous weeks' results (if `week > 1`)
 * Writing out the results
 
 ## `calculations` package
### `Calculations.scala`
This object includes the methods for scoring.  Most are exact match algorithms, but
some questions have multiple possible answers.  Both cases are handled in a UDF.  Grouping the final
results by team also happen in this object.  



