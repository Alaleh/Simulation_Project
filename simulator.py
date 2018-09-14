import csv
from sampler import sampler, Sampler
from helpers import STATUS, Statistic

log_history = {}

STARTUP_MINUTES = 8 * 60
TOTAL_MINUTES = 48 * 60
TOTAL_SIMULATION_RUNS = 10


class Job(object):
    def __init__(self, name, start, end=None):
        self.name = name
        self.start = start
        self.end = end

    def __str__(self):
        return "JOB => name: {}, start: {}, end {}".format(self.name, self.start, self.end)


def add_to_global_log(time, message):
    global log_history
    if log_history.get(time):
        log_history.get(time).append(message)
    else:
        log_history[time] = [message, ]


class Motor(object):
    def __init__(self, name, status=STATUS.IDLE):
        self.name = name
        self.status = status
        self.repair_minutes_history = []

    def is_available(self):
        return self.status == STATUS.IDLE

    def __str__(self):
        return "MOTOR => name: {}, status: {}".format(self.name, self.status)


class Machine(object):
    def __init__(self, name, motor):
        self.name = name
        self.motor = motor
        self.status = STATUS.IDLE
        self.idle_minutes_history = []
        self.working_minutes_history = []
        self.down_minutes_history = []
        self.current_job = None
        self.minutes_to_finish_job = 0
        self.minutes_to_open_motor = 10
        self.minutes_to_close_motor = 10
        self.start_repair = None
        self.down_durations = []
        self.energy = 120
        self.reapir_counts = 0
        self.done_queue = []

    def process_new_job(self, time, job):
        self.minutes_to_finish_job = sampler.get_normal_sample(120, 10)
        self.current_job = job
        self.current_job.end = time + self.minutes_to_finish_job
        add_to_global_log(time,
                          "'{}' received a new job '{}' at {} and will finish it at {}".format(self.name, job.name,
                                                                                               time,
                                                                                               self.current_job.end))
        print(
            "time: {}  => '{}' received a new job '{}' at {} and will finish it at {}".format(time, self.name, job.name,
                                                                                              time,
                                                                                              self.current_job.end))
        self.status = STATUS.WORKING

    def is_available(self):
        if self.motor and self.status == STATUS.IDLE:
            return True
        return False

    def need_repair(self):
        if self.energy <= 0 and self.status == STATUS.IDLE:
            self.status = STATUS.DOWN
            return True
        return False

    def motor_is_open(self):
        return self.minutes_to_open_motor <= 0 and self.motor is None

    def open_motor(self):
        self.status = STATUS.DOWN
        openned_motor = self.motor
        self.motor = None
        self.minutes_to_open_motor = 10
        self.reapir_counts += 1
        return openned_motor

    def motor_is_closed(self):
        return self.minutes_to_close_motor <= 0

    def close_motor(self, motor):
        self.status = STATUS.DOWN
        self.motor = motor
        self.motor.status = STATUS.WORKING
        self.minutes_to_close_motor = 10
        self.energy = 120

    def tick(self, time):
        if self.status == STATUS.DOWN:
            self.down_minutes_history.append(time)
            if self.motor:
                self.minutes_to_close_motor -= 1
                if self.minutes_to_close_motor <= 0:
                    self.status = STATUS.IDLE
                    if time - self.start_repair > 20:
                        self.down_durations.append(time - self.start_repair)
                    self.start_repair = None
                    return "MOTOR_CLOSED"
            else:
                self.minutes_to_open_motor -= 1
                if self.minutes_to_open_motor == 0:
                    add_to_global_log(time, "'{}' motor is opened".format(self.name))
                    print("time: {} => '{}' motor is opened".format(time, self.name))
                    self.start_repair = time - 10
                    return "MOTOR_OPENED"

        elif self.status == STATUS.WORKING:
            self.working_minutes_history.append(time)
            self.minutes_to_finish_job -= 1
            if self.minutes_to_finish_job == 0:
                self.status = STATUS.IDLE
                self.current_job.end = time + 1
                self.done_queue.append(self.current_job)
                print("time: {} => '{}' finished job '{}' at {}".format(time + 1, self.name, self.current_job.name,
                                                                        time + 1))
                add_to_global_log(time + 1, "'{}' finished job '{}'".format(self.name, self.current_job.name))
                return "MACHINE_FINISHED_A_JOB"
            self.energy -= 1
        elif self.status == STATUS.IDLE:
            self.idle_minutes_history.append(time)

    def __str__(self):
        return "MACHINE => name: {}, status: {}".format(self.name, self.status)


class RepairJob(object):
    def __init__(self, name, start, end=None, priority=1, repair_object=None):
        self.name = name
        self.start = start
        self.end = end
        self.priority = priority
        self.duration = None
        self.repair_object = None

    def set_duration(self, duration):
        self.duration = duration

    def __str__(self):
        return "REPAIR JOB => name: {}, start: {}, duration: {}, priority: {}".format(self.name, self.start,
                                                                                      self.duration, self.priority)


class RepairMan(object):
    def __init__(self):
        self.minutes_to_finish_rest = 15
        self.minutes_to_finish_job = 0
        self.current_job = None
        self.energy = 105
        self.status = STATUS.IDLE
        self.priority_queue = []
        self.ordinary_queue = []
        self.finished_job = None
        self.stopped_while_had_job = 0
        self.total_rests = 0
        self.done_queue = []

    def is_available(self):
        return self.energy > 0 and self.status == STATUS.WORKING

    def is_done_working(self):
        return self.minutes_to_finish_job <= 0

    def is_done_resting(self):
        return self.minutes_to_finish_rest <= 0

    def is_working(self):
        return self.status == STATUS.WORKING

    def is_resting(self):
        return self.status == STATUS.DOWN

    def need_resting(self):
        return self.energy <= 0

    def set_status_to_resting(self):
        self.energy = 105
        self.minutes_to_finish_rest = 15
        self.total_rests += 1
        if self.current_job is not None:
            print("repair man => have a job but have to rest")
            self.stopped_while_had_job += 1
        self.status = STATUS.DOWN

    def set_status_to_working(self):
        self.energy = 105
        self.status = STATUS.WORKING

    def get_next_job(self):
        job = None
        if len(self.priority_queue):
            job = self.priority_queue.pop()
        elif len(self.ordinary_queue):
            job = self.ordinary_queue.pop()
        return job

    def set_next_job(self, job):
        self.status = STATUS.WORKING
        self.finished_job = None
        self.current_job = job
        self.minutes_to_finish_job = job.duration

    def work(self):
        self.minutes_to_finish_job -= 1
        self.energy -= 1

    def rest(self):
        self.minutes_to_finish_rest -= 1

    def log(self, time, message):
        global log_history
        log_message = "time: {}  => repair man: {}".format(time, message)
        if log_history.get(time):
            log_history.get(time).append("repair man: {}".format(message))
        else:
            log_history[time] = ["repair man: {}".format(message), ]
        print(log_message)

    def tick(self, time):

        if self.need_resting():
            self.set_status_to_resting()
            self.log(time, 'going to rest')
            return "START_REPAIRMAN_REST"

        elif self.is_resting():
            self.rest()
            if self.is_done_resting():
                self.log(time, 'finished resting')
                self.status = STATUS.IDLE
                self.energy = 105
                if self.current_job is not None:
                    self.set_status_to_working()
                    self.log(time, 'going to continue previous work')
                    return "CONTINUE_REPAIR_AFTER_REST"
                return "END_REPAIRMAN_REST"

        elif self.is_working():
            self.work()
            if self.is_done_working():
                self.log(time, 'finished working on {}'.format(self.current_job.name))
                self.finished_job = self.current_job
                self.finished_job.end = time
                self.done_queue.append(self.finished_job)
                self.current_job = None
                self.status = STATUS.IDLE
                return "FINISH_REPAIR"

        else:
            if self.current_job is None:
                next_job = self.get_next_job()
                if next_job:
                    self.log(time,
                             'going to do a new job => {} with duration {}'.format(next_job.name, next_job.duration))
                    self.set_next_job(next_job)
                    return "START_NEW_REPAIR"
                else:
                    self.status = STATUS.IDLE
            else:
                self.set_status_to_working()
                self.log(time, 'going to continue previous work')


class Simulator(object):
    def __init__(self, number=1):
        self.number = number
        self.sampler = Sampler()
        self.total_jobs = 0
        self.time = 0
        self.total_priority_repairs = 0
        self.repair_man = RepairMan()
        self.motors = self._create_motors()
        self.machines = self.create_machines()
        self.jobs = self.create_jobs()
        self.ordinary_repairs = []
        self.priority_repairs = self.create_priority_repairs()
        self.statistics = []
        self.machine_queue = []
        global log_history
        log_history = {}

    @staticmethod
    def _create_motors():
        motors = [
            Motor("Motor_1", STATUS.WORKING),
            Motor("Motor_2", STATUS.WORKING),
            Motor("Motor_3", STATUS.WORKING),
            Motor("Motor_4", STATUS.IDLE),
            Motor("Motor_5", STATUS.IDLE)
        ]
        return motors

    def create_machines(self):
        machine_1 = Machine("Machine_1", self.motors[0])
        machine_2 = Machine("Machine_2", self.motors[1])
        machine_3 = Machine("Machine_3", self.motors[2])
        return [machine_1, machine_2, machine_3]

    def get_current_statisics(self):
        m1_downs = len(self.machines[0].down_durations) if len(self.machines[0].down_durations) else 1
        m2_downs = len(self.machines[1].down_durations) if len(self.machines[1].down_durations) else 1
        m3_downs = len(self.machines[2].down_durations) if len(self.machines[2].down_durations) else 1
        data = {
            "M1": self.machines[0].status,
            "M2": self.machines[1].status,
            "M3": self.machines[2].status,
            "M1_DOWN_COUNTS": len(self.machines[0].down_durations),
            "M2_DOWN_COUNTS": len(self.machines[1].down_durations),
            "M3_DOWN_COUNTS": len(self.machines[2].down_durations),
            "M1_DOWN_DURATION_AVERAGE": sum(self.machines[0].down_durations) * 1.0 / m1_downs,
            "M2_DOWN_DURATION_AVERAGE": sum(self.machines[1].down_durations) * 1.0 / m2_downs,
            "M3_DOWN_DURATION_AVERAGE": sum(self.machines[2].down_durations) * 1.0 / m3_downs,
            "MACHINE_QUEUE_SIZE": len(self.machine_queue),
            "MACHINE_QUEUE_SIZE_PER_MACHINE": len(self.machine_queue) * 1.0 / len(self.machines),
            "PRIORITY_REPAIR_QUEUE_SZIE": len(self.repair_man.priority_queue),
            "ORDINARY_REPAIR_QUEUE_SIZE": len(self.repair_man.ordinary_queue),
            "WORKING_MOTORS": sum([1 for m in self.motors if m.status != STATUS.DOWN]),
            "TOTAL_PRIRORY_REPAIRS": self.total_priority_repairs,
            "TOTAL_ORDERS": self.total_jobs,
            "REPAIR_MAN_STATUS": self.repair_man.status,
            "REPAIR_MAN_RESTS": self.repair_man.total_rests
        }
        return data

    def take_snapshot(self, event):
        if self.time < STARTUP_MINUTES:
            return
        time = self.time
        event = event
        data = self.get_current_statisics()
        new_statistic = Statistic(time, event, data)
        self.statistics.append(new_statistic)

    def save_statistics_to_csv(self):
        global log_history
        with open('simulation_{}.csv'.format(self.number), 'w') as f:
            # f.write("time,event,M1,M2,M3,REPAIR_MAN\n")
            headers = ['TIME', 'FEL', "M1", "M2", "M3",
                       "M1_DOWN_COUNTS", "M2_DOWN_COUNTS", "M3_DOWN_COUNTS",
                       "M1_DOWN_DURATION_AVERAGE", "M2_DOWN_DURATION_AVERAGE", "M3_DOWN_DURATION_AVERAGE",
                       "MACHINE_QUEUE_SIZE", "PRIORITY_REPAIR_QUEUE_SZIE",
                       "ORDINARY_REPAIR_QUEUE_SIZE", "WORKING_MOTORS", "TOTAL_PRIRORY_REPAIRS",
                       "TOTAL_ORDERS", "REPAIR_MAN_STATUS", "REPAIR_MAN_RESTS", "LOG"]
            f.write(','.join(headers) + "\n")
            for stat in self.statistics:
                stat.data["LOG"] = " || ".join(log_history.get(stat.time, []))
                output = str(stat.time) + "," + str(stat.event) + ","
                for column in headers[2:]:
                    output = output + str(stat.data[column]) + ","
                output = output[:-1] + "\n"
                f.write(output)

    def create_jobs(self):
        job_times = [1] + self.sampler.get_exponential_samples(3, 2000)
        jobs = []
        for i, time in enumerate(job_times):
            jobs.append(Job(name="job_{}".format(i + 1), start=time))
        return jobs

    def create_priority_repairs(self):
        priority_repair_times = [1] + self.sampler.get_triangular_samples(20, 50, 80, 60)
        priority_repair_durations = sampler.get_exponential_samples(20, len(priority_repair_times), acc=False)
        repairs = []
        for i, time in enumerate(priority_repair_times):
            new_repair_job = RepairJob(name="priority_repair_{}".format(i + 1), start=time, priority=100)
            new_repair_job.set_duration(priority_repair_durations[i])
            repairs.append(new_repair_job)
        return repairs

    def get_new_priority_repair_job(self):
        for i, job in enumerate(self.priority_repairs):
            if job.start == self.time:
                return self.priority_repairs.pop(i)

    def get_new_ordinary_repair_job(self):
        for i, job in enumerate(self.ordinary_repairs):
            if job.start == self.time:
                return self.ordinary_repairs.pop(i)

    def new_repair_job_exists(self):
        priority_repair_job = self.get_new_priority_repair_job()
        ordinary_repair_job = self.get_new_ordinary_repair_job()
        job_exists = False
        if priority_repair_job:
            self.total_priority_repairs += 1
            self.repair_man.priority_queue.append(priority_repair_job)
            job_exists = True
        if ordinary_repair_job:
            self.repair_man.ordinary_queue.append(ordinary_repair_job)
            job_exists = True
        return job_exists

    def new_job_exists(self):
        if len(self.machine_queue):
            return True, 0
        elif self.time in map(lambda x: x.start, self.jobs):
            return True, 1
        else:
            return False, 0

    def get_available_motors(self):
        available_motors = []
        for motor in self.motors:
            if motor.is_available():
                available_motors.append(motor)

        # assert len(available_motors) > 1
        return available_motors

    def new_priority_reapir_exists(self):
        if self.time in map(lambda x: x.start, self.priority_repairs):
            return True
        else:
            return False

    def find_ready_machine(self):
        for machine in self.machines:
            if machine.is_available():
                return machine

    def get_new_job(self):
        if len(self.machine_queue):
            return self.machine_queue[0], True
        else:
            for i, job in enumerate(self.jobs):
                if job.start == self.time:
                    return self.jobs.pop(i), False
        return None, None

    def is_a_machine_queue_job(self, new_job):
        for job in self.machine_queue:
            if new_job.name == job.name:
                return True
        return False

    def remove_job_from_machine_queue(self, new_job):
        for i, job in enumerate(self.machine_queue):
            if job.name == new_job.name:
                self.machine_queue.pop(i)

    def add_job_to_machine_queue(self, new_job):
        job_exists = False
        for job in self.machine_queue:
            if job.name == new_job.name:
                job_exists = True
        if not job_exists:
            self.log("adding {} to machine queue".format(new_job.name))
            self.machine_queue.append(new_job)

    def assign_new_job_to_machines(self):
        new_job, is_waiting_job = self.get_new_job()
        ready_machine = self.find_ready_machine()
        if ready_machine:
            self.log("going to assign job: {} to machine: {}".format(new_job.name, ready_machine.name))
            if self.is_a_machine_queue_job(new_job):
                self.remove_job_from_machine_queue(new_job)
            ready_machine.process_new_job(self.time, new_job)
        else:
            if is_waiting_job == False:
                self.log("all machines are unavailable".format(new_job.name))
            self.add_job_to_machine_queue(new_job)

    def get_ready_to_be_closed_motors(self):
        ready_machines = []
        for machine in self.machines:
            if machine.motor_is_open():
                ready_machines.append(machine)
        return ready_machines

    def get_repair_needed_machines(self):
        need_repaired_machines = []
        for machine in self.machines:
            if machine.need_repair():
                need_repaired_machines.append(machine)
                add_to_global_log(self.time, '{} needs a repair service'.format(machine.name))
                self.log("'{}' needs a repair service".format(machine.name))
        return need_repaired_machines

    def add_motor_to_repair_list(self, motor):
        duration = sampler.get_uniform_sample(10, 50)
        repair_job = RepairJob(name="repair_{}".format(motor.name), start=self.time + 10, priority=1)
        repair_job.repair_object = motor
        repair_job.set_duration(duration)
        self.ordinary_repairs.append(repair_job)

    def update_machines(self):
        repair_needed_machines = self.get_repair_needed_machines()
        ready_to_attach_motors = self.get_ready_to_be_closed_motors()
        for machine in repair_needed_machines:
            corrupt_motor = machine.open_motor()
            add_to_global_log(self.time, 'opening motor {} from {}'.format(corrupt_motor.name, machine.name))
            self.log("opening motor '{}' from machine '{}' ".format(corrupt_motor.name, machine.name))
            corrupt_motor.status = STATUS.DOWN
            assert corrupt_motor is not None
            self.add_motor_to_repair_list(corrupt_motor)

        for machine in ready_to_attach_motors:
            available_motors = self.get_available_motors()
            if len(available_motors) == 0:
                self.log('no motor is available to substitute')
            else:
                available_motor = available_motors.pop()
                machine.close_motor(available_motor)
                add_to_global_log(self.time, 'closing motor {} for {}'.format(available_motor.name, machine.name))
                self.log("closing motor '{}' for '{}' ".format(available_motor.name, machine.name))

    def tick_machines(self):
        for machine in self.machines:
            res = machine.tick(self.time)
            if res:
                self.take_snapshot(res)

    def tick_repair_man(self):
        res = self.repair_man.tick(self.time)
        if res:
            self.take_snapshot(res)

    def update_repair_man(self):
        finished_job = self.repair_man.finished_job
        if finished_job:
            if finished_job.repair_object:
                finished_job.repair_object.status = STATUS.IDLE
            self.repair_man.finished_job = None

    def log(self, message):
        global log_history
        log_message = "time: {}  => {}".format(self.time, message)
        if log_history.get(self.time):
            log_history.get(self.time).append(message)
        else:
            log_history[self.time] = [message, ]
        print(log_message)

    def get_total_experiment_stats(self):
        data = {}
        data["machines_idle_sum"] = sum([len(m.idle_minutes_history) for m in self.machines])
        data["machines_idle_average"] = data['machines_idle_sum'] / float(len(self.machines))
        data["machines_idle_per_minute"] = 1.0 / len(self.machines) * data['machines_idle_sum'] / self.time

        data["machines_down_minutes_sum"] = sum([len(m.down_minutes_history) for m in self.machines])
        data["machines_down_minutes_per_machine"] = data['machines_down_minutes_sum'] / float(len(self.machines))
        data["machines_down_per_minute"] = 1.0 / len(self.machines) * data['machines_down_minutes_sum'] / self.time

        data["motors_repair_time_sum"] = sum(
            [j.end - j.start for j in self.repair_man.done_queue if j.repair_object is not None])
        data["motors_repair_time_average"] = sum(
            [j.end - j.start for j in self.repair_man.done_queue if j.repair_object is not None]) * 1.0 / len(
            self.repair_man.done_queue)

        data["stopped_repairs_for_rest"] = self.repair_man.stopped_while_had_job
        data["stopped_repairs_for_rest_ratio"] = self.repair_man.stopped_while_had_job * 1.0 / len(
            self.repair_man.done_queue)

        job_sums = []
        for m in self.machines:
            cnt = 0
            for j in m.done_queue:
                cnt += (j.end - j.start)
            job_sums.append(cnt)

        # sum([sum([j.end - j.start] for j in m.done_queue) for m in self.machines]) / len(self.machines)
        data["order_process_time_sum"] = sum(job_sums) / float(len(self.machines))
        return data, ["machines_idle_sum", "machines_idle_average", "machines_idle_per_minute",
                      "machines_down_minutes_sum", "machines_down_minutes_per_machine", "machines_down_per_minute",
                      "motors_repair_time_sum", "stopped_repairs_for_rest", "stopped_repairs_for_rest_ratio",
                      "order_process_time_sum"]

    def get_total_stats(self):
        data, headers = self.get_total_experiment_stats()
        return data, headers

    def run(self):
        for current_time in range(1, TOTAL_MINUTES):
            self.time = current_time
            job_exists, job_is_new = self.new_job_exists()
            if job_exists:
                if job_is_new:
                    self.log('new job request arrived')
                    self.total_jobs += 1
                    self.take_snapshot('NEW_ORDER')
                self.assign_new_job_to_machines()

            self.tick_machines()
            self.update_machines()

            if self.new_repair_job_exists():
                self.log("new repair job arrived")
                self.take_snapshot('NEW_REPAIR')

            self.tick_repair_man()
            self.update_repair_man()
        self.save_statistics_to_csv()
        return self.get_total_stats()


def save_simulation_stats(headers, runs):
    with open("stats.csv", "w") as f:
        f.write("simulation_number," + ','.join(headers) + "\n")
        for n, data in enumerate(runs):
            output = str(n + 1) + ","
            for h in headers:
                output = output + str(data[h]) + ','
            f.write(output[:-1] + "\n")


if __name__ == "__main__":
    simulation_stats = []
    header = None
    for i in range(1, TOTAL_SIMULATION_RUNS + 1):
        simulator = Simulator(i)
        res, header = simulator.run()
        simulation_stats.append(res)
    save_simulation_stats(header, simulation_stats)
