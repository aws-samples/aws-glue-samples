# Data Integration with Find Matches Example Notebooks

Learn how to clean and prepare your data for analysis using a machine-learning transform called FindMatches for deduplication and finding matching records. Teach the system your criteria for calling a pair of records a match, and build a transform job that you can use to find duplicate records within a database or matching records across two databases.

In this example, we will use the DBLP/Scholar dataset of academic works and attempt to use the FindMatches algorithm and AWS Glue to integrate them together to find links between the two different data sets.  

## Getting Started

1. Create Glue Dev Endpoint. Make sure you use G.2X instances and that the assigned security role has full S3 access
2. Connect to that Glue Dev Endpoint with a SageMaker Notebook.
3. Make sure that your SageMaker Notebook's IAM role has S3 Write access (E.g. by attaching the S3FullAccess policy)
5.  Make sure that your Notebook's IAM role has the GlueServiceRole attached as well since we will be making some Glue calls
6. Create or allocate a bucket/folder for your files and edit the variables in the notebook as appropriate.
7. Create a database for your files and edit the glue_database variable in the notebook if you use a name different than 'reinvent-2019'
8. Upload each notebook in this repository and begin working through the examples in order, beginning with the "Step 1" notebook.