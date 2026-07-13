# Insufficient Memory<a name="EN-US_TOPIC_0289900068"></a>

## Symptom<a name="en-us_topic_0283137168_section4753114614509"></a>

The client or logs contain the error message  **memory usage reach the max\_dynamic\_memory**.

## Cause Analysis<a name="en-us_topic_0283137168_section31031614204014"></a>

The possible cause is that the value of the GUC parameter  **max\_process\_memory**  is too small. This parameter limits the maximum memory that can be used by an openGauss instance.

## Procedure<a name="en-us_topic_0283137168_section12618818144413"></a>

Use the  **gs\_guc**  tool to adjust the value of  **max\_process\_memory**. Note that you need to restart the instance for the modification to take effect.
