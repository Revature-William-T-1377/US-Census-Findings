# US Census Findings:
- This repository contains all the code used in Project 3 of Revatures 2022 big data cohort. 
This project scrapes census data and uses spark to answer some analytical questions based 
on the data. Some of the questions we asked include:
  - What was the total population of each state in 2000, 2010, and 2020?
  - Which region has the highest population?
  - Which region is growing the fastest?
  - Are there any states with a decreasing population?
  - What are the populations of different ethnicities and races?
  - How does the predicted population of 2020 compare to the actual population in 2020?
  - How did each states population change throughout the decades?
  - Which states are growing the fastest?
  - What predictions about the future population can we make based on this data?

## What's in this repository:
- [src/main/scala/etl](https://github.com/Revature-William-T-1377/US-Census-Findings/tree/testing/src/main/scala/etl)
contains all the code used for data scraping and ETL operations.
- [src/main/scala/queries](https://github.com/Revature-William-T-1377/US-Census-Findings/tree/testing/src/main/scala/queries)
contains all the queries we ran to answer analytical questions.
- [src/main/scala/sparkConnector](https://github.com/Revature-William-T-1377/US-Census-Findings/tree/testing/src/main/scala/sparkConnector)
contains code used to create a spark connection as well as code for starting all scraping and etl operations.
- [src/main/scala/Main.scala](https://github.com/Revature-William-T-1377/US-Census-Findings/blob/testing/src/main/scala/Main.scala)
is the main scala program that runs queries and outputs the results.
