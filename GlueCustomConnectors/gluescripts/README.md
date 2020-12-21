# Glue Script Examples

Some basic Glue job Scripts are provided here to provide some code examples of each connector types w/o a catalog connection for the connector.

These scripts are intended to be minimal to highlight the custom connector related code snippet in Glue job. 
To view a full-context Glue job script with your connector, you may go through Marketplace or BYOC workflow in Glue Studio with a connector and check the rendered job script after creating the job.

 - [Scripts Without Connections](withoutConnection/)

   Glue job examples without specifying connectionName in connection_options. 
   You will need to manually specify connection information in connection_options such as className, url (for JDBC data source) and secretId.

 - [Scripts With Connections](withConnection/)

   Glue job examples with connectionName specified in connection_options for the specifc data source connection with the custom connector. 
   Connection information, such as className, url (for JDBC data source) and secretId, is encapsulated in the catalog connection.