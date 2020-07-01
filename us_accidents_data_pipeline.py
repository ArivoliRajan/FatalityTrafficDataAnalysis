
from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql.functions import *

# Analysis of us_fatal_Accident_dataset for the year 2016
class FatalAccidentAnalysis(object):

    def __init__(self, acc_count_table,accidents_table,usa_states_code_table, states_data_url, accidents_data_url, bucket):
        self.spark = SparkSession \
                    .builder \
                    .master('yarn')\
                    .appName('us_accidents_data_pipeline_demo') \
                    .getOrCreate()
        self.acc_count_table = acc_count_table
        self.accidents_table = accidents_table
        self.usa_states_code_table = usa_states_code_table
        self.states_data_url = states_data_url
        self.accidents_data_url = accidents_data_url
        # Use the Cloud Storage bucket for temporary BigQuery export data used
        # by the connector.
        self.spark.conf.set('temporaryGcsBucket', bucket)

    def delete_bigquery_tables(self):
        # Construct a BigQuery client object.
        client = bigquery.Client()

        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        client.delete_table(self.acc_count_table, not_found_ok=True)  # Make an API request.
        # print("Deleted table '{}'.".format(table_id))

        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        client.delete_table(self.accidents_table, not_found_ok=True)  # Make an API request.
        # print("Deleted table '{}'.".format(table_id))

        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        client.delete_table(self.usa_states_code_table, not_found_ok=True)  # Make an API request.
        # print("Deleted table '{}'.".format(table_id))

    def write_data_to_bigquery(self):

        # Saving the data to BigQuery
        self.accidents.write.format('bigquery')\
                    .option('table', self.accidents_table)\
                    .save()
        # Saving the data to BigQuery
        self.states_name.write.format('bigquery')\
                    .option('table', self.usa_states_code_table)\
                    .save()


    def get_most_accident_top_10_states(self):
        # Top 10 states with most accidents

        states_colmn = self.accidents.select('STATE')
        allstates = self.states_name.join(states_colmn, states_colmn.STATE == self.states_name.state_number)
        grp = allstates.groupBy(allstates.state_name).agg(count(lit(1)).alias('acc_count'))
        res = grp.sort(grp.acc_count.desc()).limit(10)
        res.show(10)
        res.explain()
        res.printSchema()



        # Saving the data to BigQuery
        res.write.format('bigquery')\
                    .option('table', self.acc_count_table)\
                    .save()

    def read_data_from_bigquery(self):
        # Read data from BigQuery.
        accident_states = self.spark.read.format('bigquery')\
                                    .option('table', self.acc_count_table)\
                                    .load()
        accident_states.createOrReplaceTempView('statewise_accidents')
        type(accident_states)

        accident_states.count()

    def read_data_from_gcs(self):

        # Read the us_fatal_Accident_dataset from the Cloud Storage bucket
        states_csv = self.spark.read.csv(self.states_data_url, schema='''
                                                state_name string,
                                                state_code string,
                                                state_number int''').cache()
        # Change the file format to parquet
        states_csv.coalesce(1).write.mode("overwrite").parquet('gs://ararivolidemobucket/statescodeparquet')
        self.states_name = self.spark.read.parquet('gs://ararivolidemobucket/statescodeparquet')

        # Read the us_fatal_Accident_dataset from the Cloud Storage bucket
        accidents_csv = self.spark.read.csv(self.accidents_data_url, header=True, inferSchema=True).cache()

        # Change the file format to parquet
        accidents_csv.coalesce(1).write.mode("overwrite").parquet('gs://ararivolidemobucket/accidentsparquet')
        self.accidents = self.spark.read.parquet('gs://ararivolidemobucket/accidentsparquet')

        self.accidents.printSchema()
        self.accidents.take(5)


    def run(self):
        self.delete_bigquery_tables()
        self.read_data_from_gcs()
        self.write_data_to_bigquery()
        self.get_most_accident_top_10_states()
        self.read_data_from_bigquery()

        print('Results write to db finished.')

def main():
    # Use the Cloud Storage bucket for temporary BigQuery export data used
    # by the connector.
    bucket = "bucketfortemp"

    # BigQuery Table names.
    accident_count_tab = 'dataprocproject-275812.us_fatal_accidents.accident_count'
    accidents_2016     = 'dataprocproject-275812.us_fatal_accidents.us_fatal_accidents_2016'
    usa_states_code    = 'dataprocproject-275812.us_fatal_accidents.us_states_code'

    # Traffic data in GCS
    states_data_url = "gs://ararivolidemobucket/CodeOfStates.csv"
    accidents_data_url = "gs://ararivolidemobucket/FatalAccidents2016.csv"


    process = FatalAccidentAnalysis(accident_count_tab, accidents_2016, usa_states_code, states_data_url, accidents_data_url, bucket)
    process.run()


if __name__ == '__main__':
    main()


