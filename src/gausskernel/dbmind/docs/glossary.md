# DBMind Glossary

*If you add new entries, keep the alphabetical sorting!*

## Glossary
   ``agent``
      Firstly, DBMind diagnoses the database instance by using monitoring metrics, then stores the diagnosis results. But if users want DBMind to perform some operations which can heal the database instance, under this scenario, DBMind needs an agent to receive command from DBMind then perform it. This is the responsibility of the agent.
      
   ``alarm``
      When DBMind detects abnormal events, the notification generated is called alarm.
      Alarm sources include many aspects, such as from historical logs, anomalies from historical KPI, and forecasts of KPI.

   ``anomaly detection``
      Anomaly Detection (AD, aka outlier analysis) is a step in data mining that identifies events, and/or observations that deviate from a dataset's normal behavior. In the DBMind, we use it to detect abnormal events from monitoring metrics.
      
   ``component``
      Plugins of DBMind. The components are all independent functionalities. Users can execute them by using shell command `gs_dbmind component`, also can call/import them from DBMind.
      
   ``confile``
      The name of configuration file.

   ``confpath``
      The directory path which stores common data, including configuration, log, pid, etc.
     
   ``exporter``
      [Prometheus](https://prometheus.io/)'s plugin. The purpose of exporters is to scrape metrics from monitored object. See more via [Prometheus exporter](https://prometheus.io/docs/instrumenting/exporters/).
      
   ``metadatabase``
      The database which you configure in the configuration file is to store monitoring, prediction and forecast result in periodical tasks. Hence, the response time for users to browse the results is quicker.

