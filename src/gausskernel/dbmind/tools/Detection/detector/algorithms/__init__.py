from .fb_prophet import FacebookProphet


def forecast_algorithm(method):
    if method == 'fbprophet':
        return FacebookProphet
    else:
        raise Exception('No {method} forecast method.'.format(method=method))
