# sink parameters

tenant
namespace
name
inputs
className           com.eg.dataeng.BigTableSink
parallelism 
autoAck 

projectId                   eg-gcp-liverextest-test
instanceId                  send-time-bt
tableName                  coupon-offer-bt-table
tableColumnFamilies       coupon-offer-cols
credentialJsonString        required
userAgent                  coupon-offer-bt-table

#example input to sink, only COLUMN_FAMILY/COLUMNS that are being add/deleted need to be specified, to delete a column, set value to null
```commandline
{
  "row_key":"<KEY_VALUE>", 
  "row":{
    "<COLUMN_FAMILY_1>":{
      "<COLUMN_1>":"value",
      "<COLUMN_2>":"value",
      "<COLUMN_3>":"value",
      "<COLUMN_4>":"value"
    },
    "<COLUMN_FAMILY_2>":{
      "<COLUMN_1>":"value",
      "<COLUMN_2>":"value",
      "<COLUMN_3>":"value",
      "<COLUMN_4>":"value"
    },
    .
    .
    .,
    "<COLUMN_FAMILY_N>":{
      "<COLUMN_1>":"value",
      "<COLUMN_2>":"value",
      "<COLUMN_3>":"value",
      "<COLUMN_4>":"value"
    }
  }
}
```

