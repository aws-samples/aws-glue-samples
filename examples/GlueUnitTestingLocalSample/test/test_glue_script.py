import importlib
from unittest.mock import patch

from pyspark import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


class TestGlueScripts:

    def test_glue_script(self, read_from_options_responses, read_from_catalog_responses, write_from_options_mock, purge_s3_mock):
        read_from_options_responses.append([{"id": 4545, "value": "1234"}, {"id": 123213, "value": "abcdef"}])
        read_from_catalog_responses['corey-reporting-db.cust-corey_customer'] = [{"customerId": "1", "firstname": "John", "lastname": "Smith"}]
        with patch("sys.argv", ["", "--JOB_NAME", "myjob"]):
            # Run script
            importlib.import_module('my_script')

        purge_s3_mock.assert_called_with('s3://outputbucket/datapath', {'retentionPeriod': 0})

        assert 2 == write_from_options_mock.call_count
        first_call_args = write_from_options_mock.call_args_list[0][1]
        # Just validating here the type and the data but could validate all the parameters passed on the write calls
        assert "s3" == first_call_args["connection_type"]
        assert [Row(id=4545, value='1234'), Row(id=123213, value='abcdef')] == first_call_args["frame"].toDF().collect()

        second_call_args = write_from_options_mock.call_args_list[1][1]
        assert "postgresql" == second_call_args["connection_type"]
        assert StructType([
            StructField('customerId', IntegerType(), True),
            StructField('firstname', StringType(), True),
            StructField('lastname', StringType(), True)]) == second_call_args["frame"].toDF().schema
