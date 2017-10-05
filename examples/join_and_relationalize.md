# Joining, Filtering, and Loading Relational Data with AWS Glue

This example shows how to do joins and filters with transforms entirely on DynamicFrames.
It also shows you how to create tables from semi-structured data that can be loaded into
relational databases like Redshift.

The associated Python file in the examples folder is:

    join_and_relationalize.py

### 1. Crawl our sample dataset

The dataset we'll be using in this example was downloaded from the [EveryPolitician](http://everypolitician.org)
website into our sample-dataset bucket in S3, at:

    s3://awsglue-datasets/examples/us-legislators/all/

It contains data in JSON format about United States legislators and the seats they have held
in the the House of Representatives and the Senate, and has been modified somewhat for purposes
of this tutorial.

In our example code, we are assuming that you have created an AWS S3 target bucket and folder,
which we refer to as `s3://glue-sample-target/output-dir/`.

The first step is to crawl this data and put the results into a database called `legislators`
in your Data Catalog, as described [here in the Developer Guide](http://docs.aws.amazon.com/glue/latest/dg/console-crawlers.html).
The crawler will create the following tables in the `legislators` database:

 - `persons_json`
 - `memberships_json`
 - `organizations_json`
 - `events_json`
 - `areas_json`
 - `countries_r_json`

This is a semi-normalized collection of tables containing legislators and their histories.

### 2. Spin up a DevEndpoint and notebook to work with

An easy way to debug your pySpark ETL scripts is to create a `DevEndpoint', spin up and attach a Zeppelin notebook server to
the endpoint, and edit and refine the scripts in the notebook. Make sure that the role for DevEndpoint has write access to 
temporary directory paths used for relationalize and Redshift database specified later in your script. 
You can set this up through the AWS Glue console, as described
[here in the Developer Guide](http://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-prerequisites.html).

### 3. Getting started

We will write a script that:

1. Combines persons, organizations, and membership histories into a single legislator
   history data set. This is often referred to as de-normalization.
2. Separates out the senators from the representatives.
3. Writes each of these out to separate parquet files for later analysis.

Begin by pasting some boilerplate into the DevEndpoint notebook to import the
AWS Glue libraries we'll need and set up a single `GlueContext`.

    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job

    glueContext = GlueContext(SparkContext.getOrCreate())


### 4. Checking the schemas that the crawler identified

Next, you can easily examine the schemas that the crawler recorded in the Data Catalog. For example,
to see the schema of the `persons_json` table, enter the following in your notebook:

    persons = glueContext.create_dynamic_frame.from_catalog(database="legislators", table_name="persons_json")
    print "Count: ", persons.count()
    persons.printSchema()

Here's the output from the print calls:

    Count:  1961
    root
    |-- family_name: string
    |-- name: string
    |-- links: array
    |    |-- element: struct
    |    |    |-- note: string
    |    |    |-- url: string
    |-- gender: string
    |-- image: string
    |-- identifiers: array
    |    |-- element: struct
    |    |    |-- scheme: string
    |    |    |-- identifier: string
    |-- other_names: array
    |    |-- element: struct
    |    |    |-- note: string
    |    |    |-- name: string
    |    |    |-- lang: string
    |-- sort_name: string
    |-- images: array
    |    |-- element: struct
    |    |    |-- url: string
    |-- given_name: string
    |-- birth_date: string
    |-- id: string
    |-- contact_details: array
    |    |-- element: struct
    |    |    |-- type: string
    |    |    |-- value: string
    |-- death_date: string

Each person in the table is a member of some congressional body.

To look at the schema of the `memberships_json` table, enter the following:

    memberships = glueContext.create_dynamic_frame.from_catalog(database="legislators", table_name="memberships_json")
    print "Count: ", memberships.count()
    memberships.printSchema()

The output is:

    Count:  10439
    root
    |-- area_id: string
    |-- on_behalf_of_id: string
    |-- organization_id: string
    |-- role: string
    |-- person_id: string
    |-- legislative_period_id: string
    |-- start_date: string
    |-- end_date: string

Organizations are parties and the two chambers of congress, the Senate and House.
To look at the schema of the `organizations_json` table, enter:

    orgs = glueContext.create_dynamic_frame.from_catalog(database="legislators", table_name="organizations_json")
    print "Count: ", orgs.count()
    orgs.printSchema()

The output is:

    Count:  13
    root
    |-- classification: string
    |-- links: array
    |    |-- element: struct
    |    |    |-- note: string
    |    |    |-- url: string
    |-- image: string
    |-- identifiers: array
    |    |-- element: struct
    |    |    |-- scheme: string
    |    |    |-- identifier: string
    |-- other_names: array
    |    |-- element: struct
    |    |    |-- lang: string
    |    |    |-- note: string
    |    |    |-- name: string
    |-- id: string
    |-- name: string
    |-- seats: int
    |-- type: string


### 5. Filtering

Let's only keep the fields that we want and rename `id` to `org_id`. The dataset is small enough that we can
look at the whole thing. The `toDF()` converts a DynamicFrame to a Spark DataFrame, so we can apply the
transforms that already exist in SparkSQL:

    orgs = orgs.drop_fields(['other_names',
                            'identifiers']).rename_field(
                                'id', 'org_id').rename_field(
                                   'name', 'org_name')
    orgs.toDF().show()

The output is:

    +--------------+--------------------+--------------------+--------------------+-----+-----------+--------------------+
    |classification|              org_id|            org_name|               links|seats|       type|               image|
    +--------------+--------------------+--------------------+--------------------+-----+-----------+--------------------+
    |         party|            party/al|                  AL|                null| null|       null|                null|
    |         party|      party/democrat|            Democrat|[[website,http://...| null|       null|https://upload.wi...|
    |         party|party/democrat-li...|    Democrat-Liberal|[[website,http://...| null|       null|                null|
    |   legislature|d56acebe-8fdc-47b...|House of Represen...|                null|  435|lower house|                null|
    |         party|   party/independent|         Independent|                null| null|       null|                null|
    |         party|party/new_progres...|     New Progressive|[[website,http://...| null|       null|https://upload.wi...|
    |         party|party/popular_dem...|    Popular Democrat|[[website,http://...| null|       null|                null|
    |         party|    party/republican|          Republican|[[website,http://...| null|       null|https://upload.wi...|
    |         party|party/republican-...|Republican-Conser...|[[website,http://...| null|       null|                null|
    |         party|      party/democrat|            Democrat|[[website,http://...| null|       null|https://upload.wi...|
    |         party|   party/independent|         Independent|                null| null|       null|                null|
    |         party|    party/republican|          Republican|[[website,http://...| null|       null|https://upload.wi...|
    |   legislature|8fa6c3d2-71dc-478...|              Senate|                null|  100|upper house|                null|
    +--------------+--------------------+--------------------+--------------------+-----+-----------+--------------------+

Let's look at the `organizations` that appear in `memberships`:

    memberships.select_fields(['organization_id']).toDF().distinct().show()

The output is:

    +--------------------+
    |     organization_id|
    +--------------------+
    |d56acebe-8fdc-47b...|
    |8fa6c3d2-71dc-478...|
    +--------------------+



### 6. Putting it together

Now let's join these relational tables to create one full history table of legislator
memberships and their correponding organizations, using AWS Glue.

 - First, we join `persons` and `memberships` on `id` and `person_id`.
 - Next, join the result with orgs on `org_id` and `organization_id`.
 - Then, drop the redundant fields, `person_id` and `org_id`.

We can do all these operations in one (extended) line of code:

    l_history = Join.apply(orgs,
                           Join.apply(persons, memberships, 'id', 'person_id'),
                           'org_id', 'organization_id').drop_fields(['person_id', 'org_id'])
    print "Count: ", l_history.count()
    l_history.printSchema()

The output is:

    Count:  10439
    root
    |-- role: string
    |-- seats: int
    |-- org_name: string
    |-- links: array
    |    |-- element: struct
    |    |    |-- note: string
    |    |    |-- url: string
    |-- type: string
    |-- sort_name: string
    |-- area_id: string
    |-- images: array
    |    |-- element: struct
    |    |    |-- url: string
    |-- on_behalf_of_id: string
    |-- other_names: array
    |    |-- element: struct
    |    |    |-- note: string
    |    |    |-- name: string
    |    |    |-- lang: string
    |-- contact_details: array
    |    |-- element: struct
    |    |    |-- type: string
    |    |    |-- value: string
    |-- name: string
    |-- birth_date: string
    |-- organization_id: string
    |-- gender: string
    |-- classification: string
    |-- death_date: string
    |-- legislative_period_id: string
    |-- identifiers: array
    |    |-- element: struct
    |    |    |-- scheme: string
    |    |    |-- identifier: string
    |-- image: string
    |-- given_name: string
    |-- family_name: string
    |-- id: string
    |-- start_date: string
    |-- end_date: string

Great! We now have the final table that we'd like to use for analysis.
Let's write it out in a compact, efficient format for analytics, i.e. Parquet,
that we can run SQL over in AWS Glue, Athena, or Redshift Spectrum.

The following call writes the table across multiple files to support fast parallel
reads when doing analysis later:

    glueContext.write_dynamic_frame.from_options(frame = l_history,
              connection_type = "s3",
              connection_options = {"path": "s3://glue-sample-target/output-dir/legislator_history"},
              format = "parquet")

To put all the history data into a single file, we need to convert it to a data frame, repartition it, and
write it out.

    s_history = l_history.toDF().repartition(1)
    s_history.write.parquet('s3://glue-sample-target/output-dir/legislator_single')

Or if you want to separate it by the Senate and the House:

    l_history.toDF().write.parquet('s3://glue-sample-target/output-dir/legislator_part',
                                   partitionBy=['org_name'])


### 7. Writing to Relational Databases

AWS Glue makes it easy to write it to relational databases like Redshift even with
semi-structured data. It offers a transform, `relationalize()`, that flattens DynamicFrames
no matter how complex the objects in the frame may be.

Using the `l_history` DynamicFrame in our example, we pass in the name of a root table
(`hist_root`) and a temporary working path to `relationalize`, which returns a `DynamicFrameCollection`.
We then list the names of the DynamicFrames in that collection:

    dfc = l_history.relationalize("hist_root", "s3://glue-sample-target/temp-dir/")
    dfc.keys()

The output of the `keys` call is:

    [u'hist_root', u'hist_root_contact_details', u'hist_root_links',
     u'hist_root_other_names', u'hist_root_images', u'hist_root_identifiers']


Relationalize broke the history table out into 6 new tables: a root table containing a record for each object in the
dynamic frame, and auxiliary tables for the arrays. Array handling in relational databases is often sub-optimal,
especially as those arrays become large. Separating out the arrays into separate tables makes the queries go much
faster.

Let's take a look at the separation by examining `contact_details`:

    l_history.select_fields('contact_details').printSchema()
    dfc.select('hist_root_contact_details').toDF().where("id = 10 or id = 75").orderBy(['id','index']).show()

The output of the `printSchema` and `show` calls is:

    root
    |-- contact_details: array
    |    |-- element: struct
    |    |    |-- type: string
    |    |    |-- value: string
    +---+-----+------------------------+-------------------------+
    | id|index|contact_details.val.type|contact_details.val.value|
    +---+-----+------------------------+-------------------------+
    | 10|    0|                     fax|             202-224-6020|
    | 10|    1|                   phone|             202-224-3744|
    | 10|    2|                 twitter|            ChuckGrassley|
    | 75|    0|                     fax|             202-224-4680|
    | 75|    1|                   phone|             202-224-4642|
    | 75|    2|                 twitter|              SenJackReed|
    +---+-----+------------------------+-------------------------+

The `contact_details` field was an array of structs in the original DynamicFrame.
Each element of those arrays is a separate row in the auxiliary table, indexed by
`index`. The `id` here is a foreign key into the `hist_root` table with the key
`contact_details`.

    dfc.select('hist_root').toDF().where(
        "contact_details = 10 or contact_details = 75").select(
           ['id', 'given_name', 'family_name', 'contact_details']).show()

The output is:

    +--------------------+----------+-----------+---------------+
    |                  id|given_name|family_name|contact_details|
    +--------------------+----------+-----------+---------------+
    |f4fc30ee-7b42-432...|      Mike|       Ross|             10|
    |e3c60f34-7d1b-4c0...|   Shelley|     Capito|             75|
    +--------------------+----------+-----------+---------------+

Notice in the commands above that we used `toDF()` and subsequently a `where` expression to filter for the rows that
we wanted to see.

So, joining the `hist_root` table with the auxiliary tables allows you to:

 - Load data into databases without array support.
 - Query each individual item in an array using SQL.

We already have a connection set up called `redshift3`. To create your own, see
[this topic in the Developer Guide](http://docs.aws.amazon.com/glue/latest/dg/populate-add-connection.html).
Let's write this collection into Redshift by cycling through the DynamicFrames one at a time:

    for df_name in dfc.keys():
        m_df = dfc.select(df_name)
        print "Writing to Redshift table: ", df_name
        glueContext.write_dynamic_frame.from_jdbc_conf(frame = m_df,
                                                       catalog_connection = "redshift3",
                                                       connection_options = {"dbtable": df_name, "database": "testdb"},
                                                       redshift_tmp_dir = "s3://glue-sample-target/temp-dir/")

Here's what the tables look like in Redshift. (We connected to Redshift through psql.)

    testdb=# \d
                       List of relations
     schema |           name            | type  |   owner
    --------+---------------------------+-------+-----------
     public | hist_root                 | table | test_user
     public | hist_root_contact_details | table | test_user
     public | hist_root_identifiers     | table | test_user
     public | hist_root_images          | table | test_user
     public | hist_root_links           | table | test_user
     public | hist_root_other_names     | table | test_user
    (6 rows)

    testdb=# \d hist_root_contact_details
                 Table "public.hist_root_contact_details"
              Column           |           Type           | Modifiers
    ---------------------------+--------------------------+-----------
     id                        | bigint                   |
     index                     | integer                  |
     contact_details.val.type  | character varying(65535) |
     contact_details.val.value | character varying(65535) |

    testdb=# \d hist_root
                       Table "public.hist_root"
            Column         |           Type           | Modifiers
    -----------------------+--------------------------+-----------
     role                  | character varying(65535) |
     seats                 | integer                  |
     org_name              | character varying(65535) |
     links                 | bigint                   |
     type                  | character varying(65535) |
     sort_name             | character varying(65535) |
     area_id               | character varying(65535) |
     images                | bigint                   |
     on_behalf_of_id       | character varying(65535) |
     other_names           | bigint                   |
     birth_date            | character varying(65535) |
     name                  | character varying(65535) |
     organization_id       | character varying(65535) |
     gender                | character varying(65535) |
     classification        | character varying(65535) |
     legislative_period_id | character varying(65535) |
     identifiers           | bigint                   |
     given_name            | character varying(65535) |
     image                 | character varying(65535) |
     family_name           | character varying(65535) |
     id                    | character varying(65535) |
     death_date            | character varying(65535) |
     start_date            | character varying(65535) |
     contact_details       | bigint                   |
     end_date              | character varying(65535) |

Now you can query these tables using SQL in Redshift:

    testdb=# select * from hist_root_contact_details where id = 10 or id = 75 order by id, index;

With this result:

    id | index | contact_details.val.type | contact_details.val.value
    ---+-------+--------------------------+---------------------------
    10 |     0 | fax                      | 202-224-6020
    10 |     1 | phone                    | 202-224-3744
    10 |     2 | twitter                  | ChuckGrassley
    75 |     0 | fax                      | 202-224-4680
    75 |     1 | phone                    | 202-224-4642
    75 |     2 | twitter                  | SenJackReed
    (6 rows)


### Conclusion

Overall, AWS Glue is quite flexible allowing you to do in a few lines of code, what normally would take days to
write. The entire source to target ETL scripts from end-to-end can be found in the accompanying Python file,
`join_and_relationalize.py`.

