from utils import RepeatTimer, transform_time_string
from .monitor_logger import logger


class Monitor:
    def __init__(self):
        self._tasks = dict()

    def apply(self, instance, args=None, kwargs=None):
        if instance in self._tasks:
            return False
        logger.info('add [{task}] in Monitor task......'.format(task=getattr(instance, 'metric_name')))
        interval = getattr(instance, 'forecast_interval')
        try:
            interval = transform_time_string(interval, mode='to_second')
        except ValueError as e:
            logger.error(e, exc_info=True)
            return
        timer = RepeatTimer(interval=interval, function=instance.run, args=args, kwargs=kwargs)
        self._tasks[instance] = timer
        return True

    def start(self):
        for instance, timer in self._tasks.items():
            timer.start()
            logger.info('begin to monitor [{task}]'.format(task=getattr(instance, 'metric_name')))
