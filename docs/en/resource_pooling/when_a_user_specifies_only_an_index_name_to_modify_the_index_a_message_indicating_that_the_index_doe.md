# When a User Specifies Only an Index Name to Modify the Index, A Message Indicating That the Index Does Not Exist Is Displayed<a name="EN-US_TOPIC_0291615105"></a>

## Symptom<a name="section11297759192710"></a>

When a User Specifies Only an Index Name to Modify the Index, A Message Indicating That the Index Does Not Exist Is Displayed The following provides an example:

```
-- Create a partitioned table index HR_staffS_p1_index1, without specifying index partitions.
CREATE INDEX HR_staffS_p1_index1 ON HR.staffS_p1 (staff_ID) LOCAL; 
-- Create a partitioned table index HR_staffS_p1_index2, with index partitions specified.
CREATE INDEX HR_staffS_p1_index2 ON HR.staffS_p1 (staff_ID) LOCAL 
(   
PARTITION staff_ID1_index,    
PARTITION staff_ID2_index TABLESPACE example3,    
PARTITION staff_ID3_index TABLESPACE example4 
) TABLESPACE example; 
-- Change the tablespace of index partition staff_ID2_index to example1. A message is displayed, indicating that the index does not exist.
ALTER INDEX HR_staffS_p1_index2 MOVE PARTITION staff_ID2_index TABLESPACE example1;
```

## Cause Analysis<a name="section13485101002814"></a>

The possible reason is that the user is in the public schema instead of the hr schema.

```
-- Run the following command to validate the inference. It is found that the calling is successful.
ALTER INDEX hr.HR_staffS_p1_index2 MOVE PARTITION staff_ID2_index TABLESPACE example1;
-- Change the schema of the current session to hr.
ALTER SESSION SET CURRENT_SCHEMA TO hr;
-- Run the following command to modify the index:
ALTER INDEX HR_staffS_p1_index2 MOVE PARTITION staff_ID2_index TABLESPACE example1;
```

## Procedure<a name="section12373188285"></a>

Add a schema reference to a table, index, or view. The format is as follows:

```
schema.table
```
