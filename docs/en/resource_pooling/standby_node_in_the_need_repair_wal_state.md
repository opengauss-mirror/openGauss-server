# Standby Node in the  **Need Repair \(WAL\)**  State<a name="EN-US_TOPIC_0255061693"></a>

## Symptom<a name="section19264812163110"></a>

The  **Need Repair \(WAL\)**  fault occurs on a standby node of the openGauss.

## Cause Analysis<a name="section31031614204014"></a>

The primary and standby DB instances are disconnected due to network faults or insufficient disk space. As a result, logs are not synchronized between the primary and standby DB instances, and the database cluster fails to start.

## Procedure<a name="section12618818144413"></a>

Run the  **gs\_ctl build -D**  command to rebuild the faulty node. For details, see the build parameter in the  _openGauss Tool Reference_.
