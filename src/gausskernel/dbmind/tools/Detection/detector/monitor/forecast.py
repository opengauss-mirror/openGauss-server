from detector.monitor import detect
from .monitor_logger import logger


class Forecastor:
    """
    repeat execute by Timer
    """

    def __init__(self, **kwargs):
        self.minimum_timeseries_length = 20
        self.metric_name = kwargs['metric_name']
        self.database_path = kwargs['database_path']
        self.data_handler = kwargs['data_handler']
        self.forecast_alg = kwargs['forecast_alg']
        self.forecast_period = kwargs['forecast_period']
        self.forecast_interval = kwargs['forecast_interval']
        self.data_period = kwargs['data_period']

        self.detect_basis = {'minimum': kwargs.get('minimum', None),
                             'maximum': kwargs.get('maximum', None)}

    @detect.Detector
    def run(self):
        forecast_result = {}
        with self.data_handler(self.metric_name, self.database_path) as db:
            timeseries = db.get_timeseries(period=self.data_period)
            logger.info('acquire data[{metric_name}] -> data_period: {data_period} data_length: {length}'
                        .format(metric_name=self.metric_name, 
                                data_period=self.data_period, 
                                length=len(timeseries)))
            if not timeseries:
                logger.error("can not get timeseries from table [{metric_name}] by period '{period}', "
                             "skip forecast step for [{metric_name}]".format(metric_name=self.metric_name,
                                                                             period=self.data_period))
                forecast_result['status'] = 'fail'
            else:
                try:
                    if len(timeseries) < self.minimum_timeseries_length:
                        logger.warn(
                            "the length of timeseries[{metric_name}] is too short: [{ts_length}]."
                            .format(metric_name=self.metric_name,
                                    ts_length=len(timeseries)))
                    self.forecast_alg.fit(timeseries)
                    self.forecast_period = self.forecast_period.upper()
                    date, value = self.forecast_alg.forecast(self.forecast_period)
                    logger.info("forecast[{metric_name}] -> forecast length: {length}"
                                .format(metric_name=self.metric_name,
                                        length=len(value)))
                    forecast_result['status'] = 'success'
                    forecast_result['metric_name'] = self.metric_name
                    forecast_result['detect_basis'] = self.detect_basis
                    forecast_result['future_date'] = date
                    forecast_result['future_value'] = value
                except Exception as e:
                    logger.error(e, exc_info=True)
                    forecast_result['status'] = 'fail'
        return forecast_result

    def __repr__(self):
        return 'forecastor of the metric {metric}'.format(metric=self.metric_name)
