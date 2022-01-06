import json
import apache_beam as beam
import apache_beam.io.gcp.bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import argparse

options = PipelineOptions(
    #project="york-cdf-start", #Jenkins will need
    #region="us-central1", #Jenkins will need
    temp_location="gs://sh_temp_test",
    job_name='sonja_pipeline'
)
    #temp_location="gs://york-project-bucket/sonja_hayden/dataflow/tmp/",

    # runner='DataflowRunner') #Jenkins will need
    #staging_location="gs://york-project-bucket/sonja_hayden/dataflow/staging/",
    #streaming=True,
    #save_main_session=True)

if __name__ == '__main__':
    # schema to # table to write data back into
    # TODO: this is a template, I need to alter as needed. for now just testing getting table and writing same table
    #the schema will need to be created identical to the output table, will need to create 2 schemas . fields will vary
    #output will be inthe same order
    TABLE_SCHEMA = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABlE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }
    # table specs always needed to creat tables
    # tableID can be named here if does not exist already
    table_spec = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="SH_practice_project",
        tableId="test_table"
    )
    #variable set to access BQtable1 in query
    table1 = "york-cdf-start.bigquerypython.bqtable1"
    #make sure order of output matches
    with beam.Pipeline(options=options) as pipeline:
        table_data = pipeline | 'Read tables in from BigQuery' >> beam.io.ReadFromBigQuery(
            query='SELECT name, order_id, description from york-cdf-start.bigquerypython.bqtable1',
            project='york-cdf-start', #same as in options
            use_standard_sql=True
        ) #| "print" >> beam.Map(print) #can't write to BQ and print
        #tableReference can be a PROJECT:DATASET.TABLE or DATASET.TABLE string.

       # use the split_data object, create and write data into BigQuery table
        table_data | 'test write table to BigQuery' >> beam.io.WriteToBigQuery(
            table_spec,  #
            schema=TABLE_SCHEMA,  # variable was assigned earlier
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  # must have to create table
            #with batch need to remove values if there is an error, so do NOT append
            #write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

            # probably can use default but check what default is
            # method="streaming_inserts" #raised an error, maybe due to options already set??
        )
