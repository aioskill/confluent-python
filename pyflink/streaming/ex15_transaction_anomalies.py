"""
transaction_producer.py script generates synthetic transaction data and send to kafka topic.
    - update the script to generate timestamp over last 60 days (required for long range testing)

Using flink SQL write a job that will
1. Find count by each transaction category in last 24 hours
2. Find total number of transactions and total amount in rolling 24 hours period for each customer id
3. Find avg daily amount by each customer id based on last 30 days
4. If transaction amount over rolling 24hour exceeds the avg (from step 3) x 1.5 (tolerance threshold),
   create an alert record. (final output)

Write the final output to kafka topic.

Use the following variables:
- window length
- sliding interval
- watermark
- tolerance (threshold)

Future enhancements:
- currency normalization
- threshold by transaction category
- customer age
- credit history
- geography
- cross border transaction flags
- PEP (Politically Exposed Person)



"""