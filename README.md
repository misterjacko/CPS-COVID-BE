# CPS-COVID BACK-END README #

The ETL layer for a dashboard for tracking reported COVID-19 cases at Chicago Public Schools.

## Project Intro ##

This is the back-end of the Chicago Public Schools Covid tracking website found at [cpscovid.com](https://cpscovid.com).

The purpose is to automate the ingesting and updating of the dataset used for display by the front end.

At regular intervals an [EventBridge](https://aws.amazon.com/eventbridge/) scheduled event triggers a [Lambda](https://aws.amazon.com/lambda/) function. This function pulls a public Google Sheet ([link](https://docs.google.com/spreadsheets/d/1dMtr8hhhKjPyyNg7i6V52iMQXEqa67E9iAmECeOqZ6c)) maintained by Chicago Public Schools with current totals of cases for each school for students and staff who were present in that District run (read: not a charter etc.) school. This document is updated AFTER potenitally exposed individuals in the building and classroom are notified, so data lags behind reality.

The function also pulls a csv file containing all the school's totals, previous daily counts, and location data for each school.

Both of these files (historical data and new data) are loaded into a [pandas](https://pandas.pydata.org/) dataframe and evaluated. First, the new data is validated through a number of tests fo verify that it is in a format that is expected (the process will abort if there is an error). Then, new column is added to the historical data containing the new cases at each school (historical total - new total) and then the historical total is updated to reflect the new total. The now up-to-date 'historical data' is exported as a .csv and placed back in the S3 bucket used by the front-end.

Additionally, new case data is used to generate a static html page with detailed case data from a template and format an SNS message with summary case data.

All files that may be updated when there are new case numbers are also invalidated in cloudfront so that stale data is not served.

A CloudWatch Alarm is set up to monitor the Lambda function and send out an SNS notification in the event of a failure (for whatever reason)

![infrastructure architecturepart 1](https://github.com/misterjacko/CPS-COVID-BE/blob/main/Architecture1.png)

![infrastructure architecture part 2](https://github.com/misterjacko/CPS-COVID-BE/blob/main/Architecture2.png)

The repository for the front end can be found [here](https://github.com/misterjacko/CPS-COVID-FE).

## Technology stack ##
  
The project is built with the [AWS Serverless Application Model (SAM)](https://aws.amazon.com/serverless/sam/) and consists primarily of a `template.yaml` file that describes the deployed archetecture, and two python functions that will run on [AWS Lambda](https://aws.amazon.com/lambda/). Once build and deployed to AWS, the function, IAM roles, [EventBridge (CloudWatch Events)](https://aws.amazon.com/cloudwatch/) schedule, etc. are configured and become real infrastructure.

The data update function, written in python, uses the [pandas](https://pandas.pydata.org/) framework to update 'yesterday's' dataset with the new totals that are published by the school district.

There is also an SNS subscribe function that allows users to subscribe to an SNS topic that the update functions publishes to when it detects new case data. It is also written in python.

A [CloudWatch Alarm](https://aws.amazon.com/cloudwatch/) is triggered if the function fails for any reason, and notifies me via [SNS](https://aws.amazon.com/sns/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc).

Once the dataset is updated, it is saved as the same file in a versioned [S3](https://aws.amazon.com/s3/) bucket.

## Status ##

- In active development

## Known issues/technical debt ##

- Public SNS subscription not yet publically available because I havent tested it enough yet.
- New case page that is dynamically created updates on any update and the SNS summary assume one update. Multiple updates in a day will cumulate, but that means if someone is subscribes to SNS updates, the multiple updates will be confusing.

## TODO ##

- Test and release public SNS
- Add summary tweet when there is new data with twitter API
- Find informative map layers like vaccination data to overlay.

## Contact ##

http://www.jakobondrey.com

----

## Credits and references ##

1. [CPS Dataset](https://www.cps.edu/school-reopening-2020/)
    - [-Data-](https://docs.google.com/spreadsheets/d/1dMtr8hhhKjPyyNg7i6V52iMQXEqa67E9iAmECeOqZ6c)
2. [School Loaction Dataset](https://catalog.data.gov/organization/86c0c3d9-3826-47ab-a773-6924b858dd04?groups=local&tags=cps)
    - [-Data-](https://data.cityofchicago.org/api/views/d2h8-2upd/rows.csv?accessType=DOWNLOAD)
3. [SAM](https://aws.amazon.com/serverless/sam/)
4. [pandas](https://pandas.pydata.org/)
