class ExponentialMovingAverage:

    def __init__(self, lookback: float):
        self.average: float | None = None
        self.lookback = lookback

    def record(self, value: float):
        if self.average is None:
            self.average = value
        else:
            self.average = (value - self.average) * (2.0 / (self.lookback + 1)) + self.average
