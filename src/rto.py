class RtoEstimator:
    def __init__(self, initial_rto=1.0, min_rto=0.2, max_rto=5.0,
                 clock_granularity=0.001):
        if min_rto <= 0 or max_rto < min_rto:
            raise ValueError("RTO bounds must satisfy 0 < min_rto <= max_rto")
        if clock_granularity <= 0:
            raise ValueError("clock_granularity must be positive")

        self.min_rto = min_rto
        self.max_rto = max_rto
        self.clock_granularity = clock_granularity
        self.rto = self._clamp(initial_rto)
        self.srtt = None
        self.rttvar = None
        self.samples = 0

    def _clamp(self, value):
        return max(self.min_rto, min(self.max_rto, value))

    def update(self, sample):
        """Incorporate one valid RTT sample (in seconds)."""
        if sample <= 0:
            return self.rto

        if self.srtt is None:
            self.srtt = sample
            self.rttvar = sample / 2
        else:
            deviation = abs(self.srtt - sample)
            self.rttvar = 0.75 * self.rttvar + 0.25 * deviation
            self.srtt = 0.875 * self.srtt + 0.125 * sample

        self.samples += 1
        self.rto = self._clamp(
            self.srtt + max(self.clock_granularity, 4 * self.rttvar)
        )
        return self.rto

    def backoff(self):
        """Apply exponential timeout backoff after a retransmission timeout."""
        self.rto = self._clamp(self.rto * 2)
        return self.rto
