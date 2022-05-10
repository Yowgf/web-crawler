class FakeRobotsPolicy:
    def __init__(self, delay):
        self.delay = delay

    def allowed(self, s):
        return True
