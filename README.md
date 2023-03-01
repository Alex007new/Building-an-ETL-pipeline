# Building-an-ETL-pipeline

We solve the following ETL problem: the output will be a DAG in Airflow, which will be counted every day as yesterday.

1. We process two tables in parallel.\
    In the **feed_actions** table for each user, we count the number of views and likes of the content.\
    In the **message_actions** table for each user, we count how many messages he receives and sends,
    how many people he writes to, how many people write to him. Each upload must be in a separate task.
>
2. We combine two tables into one.
>
3. In the new table, we consider all the metrics in the context of gender, age and wasps.
    We do three different tasks for each cut.
>
4. We write the final data with all the metrics into a separate table in ClickHouse.
>
5. Every day the table is updated with new data (for yesterday).
>
The structure of the final table:\
      Date - **event_date**\
      Slice name - **dimension**\
      Slice value - **dimension_value**\
      Number of views - **views**\
      Number of likes - **likes**\
      Number of messages received - **messages_received**\
      Number of messages sent - **messages_sent**\
      How many users received messages from - **users_received**\
      How many users sent a message - **users_sent**\
      Slices are **os**, **gender**, **age**
