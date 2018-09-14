class Status(object):
    WORKING = 'WORKING'
    IDLE = 'IDLE'
    DOWN = 'DOWN'


class Statistic(object):
    def __init__(self, time, event, data):
        self.time = time
        self.event = event
        self.data = data


STATUS = Status()
