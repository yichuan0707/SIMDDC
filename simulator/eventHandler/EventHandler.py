from collections import OrderedDict
from math import sqrt, ceil
from random import randint, choice
from copy import deepcopy

from simulator.Event import Event
from simulator.Result import Result
from simulator.utils import FIFO
from simulator.Log import info_logger, error_logger
from simulator.unit.Rack import Rack
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk
from simulator.unit.DiskWithScrubbing import DiskWithScrubbing
from simulator.unit.SliceSet import SliceSet


class EventHandler(object):

    def __init__(self, distributer):
        self.distributer = distributer
        self.conf = self.distributer.returnConf()
        self.drs_handler = self.conf.getDRSHandler()
        self.n, self.k = self.distributer.returnCodingParameters()
        self.slice_locations = self.distributer.returnSliceLocations()

        self.num_chunks_diff_racks = self.conf.num_chunks_diff_racks
        self.lost_slice = -100

        self.end_time = self.conf.total_time
        self.total_slices_table = self.conf.tableForTotalSlice()
        # the final total slices
        self.total_slices = self.total_slices_table[-1][2]

        # A slice is recovered when recoveryThreshold number of chunks are
        # 'lost', where 'lost' can include durability events (disk failure,
        # latent failure), as well as availability events (temporary machine
        # failure) if availabilityCountsForRecovery is set to true (see below)
        # However, slice recovery can take two forms:
        # 1. If lazyRecovery is set to false: only the chunk that is in the
        # current disk being recovered, is recovered.
        # 2. If lazyRecovery is set to true: all chunks of this slice that are
        # known to be damaged, are recovered.
        self.lazy_recovery = self.conf.lazy_recovery
        self.recovery_threshold = self.conf.recovery_threshold

        self.parallel_repair = self.conf.parallel_repair
        self.recovery_bandwidth_cap = self.conf.recovery_bandwidth_cross_rack

        # Lazy recovery threshold can be defined in one of two ways:
        #  1. a slice is recovered when some number of *durability* events
        #     happen
        #  2. a slice is recovered when some number of durability and/or
        #     availability events happen
        # where durability events include permanent machine failures, or disk
        # failures, while availabilty events are temporary machine failures
        # This parameter -- availabilityCountsForRecovery -- determines which
        # policy is followed. If true, then definition #2 is followed, else
        # definition #1 is followed.
        self.availability_counts_for_recovery = \
            self.conf.availability_counts_for_recovery

        self.queue_disable = self.conf.queue_disable
        if not self.queue_disable:
            self.bandwidth_contention = self.conf.bandwidth_contention
            # Now, we only support FIFO model for bandwidth contention
            # if self.bandwidth_contention == "FIFO":
            self.contention_model = FIFO(self.distributer.getAllRacks())

        # False means no scaling during mission time;
        # True means there is scaling during mission time.
        self.scaling = self.conf.system_scaling

        # for each block, 1 means Normal, 0 means Unavailable, -1 means Lost(caused by disk or node lost),
        # -2 means Lost(caused by LSE)
        self.status = [[1 for i in xrange(self.n)] for j in xrange(self.total_slices)]

        self.unavailable_slice_count = 0

        # [(slice_index, occur_time, caused by what kind of component failure), ...],
        # example: (13567, 12456.78, "disk 137")
        self.undurable_slice_infos = []
        self.undurable_slice_count = 0
        self.current_slice_degraded = 0
        self.current_avail_slice_degraded = 0

        # slice_index:[[failure time, recovery time],...]
        self.unavailable_slice_durations = {}

        # There is an anomaly (logical bug?) that is possible in the current
        # implementation:
        # If a machine A suffers a temporary failure at time t, and between t
        # and t+failTimeout, if a recovery event happens which affects a slice
        # which is also hosted on machine A, then that recovery event may
        # rebuild chunks of the slice that were made unavailable by machine
        # A's failure. This should not happen, as technically, machine A's
        # failure should not register as a failure until t+failTimeout.
        # This count -- anomalousAvailableCount -- keeps track of how many
        # times this happens
        self.anomalous_available_count = 0

        # instantaneous total recovery b/w, in MB/hr, not to exceed above cap
        self.current_recovery_bandwidth = 0
        # max instantaneous recovery b/w, in MB/hr
        self.max_recovery_bandwidth = 0

        self.max_bw = 0
        self.bandwidth_list = OrderedDict()

        self.total_latent_failures = 0
        self.total_scrubs = 0
        self.total_scrub_repairs = 0
        self.total_disk_failures = 0
        self.total_disk_repairs = 0
        self.total_machine_failures = 0
        self.total_machine_repairs = 0
        self.total_perm_machine_failures = 0
        self.total_short_temp_machine_failures = 0
        self.total_long_temp_machine_failures = 0
        self.total_machine_failures_due_to_rack_failures = 0
        self.total_eager_machine_repairs = 0
        self.total_eager_slice_repairs = 0
        self.total_skipped_latent = 0
        self.total_incomplete_recovery_attempts = 0

        self.total_repairs = 0
        self.total_repair_transfers = 0
        self.total_optimal_repairs = 0

        # self.slices_degraded_list = []
        # self.slices_degraded_avail_list = []

        # degraded slice statistic dict
        # self.slices_degraded_durations = {}

    def _my_assert(self, expression):
        if not expression:
            raise Exception("My Assertion failed!")
        return True

    def durableCount(self, slice_index):
        if isinstance(self.status[slice_index], int):
            return self.status[slice_index]
        else:
            return self.status[slice_index].count(1) + self.status[slice_index].count(0)

    def availableCount(self, slice_index):
        if isinstance(self.status[slice_index], int):
            return self.status[slice_index]
        else:
            return self.status[slice_index].count(1)

    def sliceRecovered(self, slice_index):
        if self.durableCount(slice_index) == self.n:
            self.current_slice_degraded -= 1
        self.sliceRecoveredAvailability(slice_index)

    def sliceDegraded(self, slice_index):
        if self.durableCount(slice_index) == self.n:
            self.current_slice_degraded += 1
        self.sliceDegradedAvailability(slice_index)

    def sliceRecoveredAvailability(self, slice_index):
        if self.k == 1:
            # replication is not affected by this
            return
        unavailable = self.n - self.availableCount(slice_index)
        if unavailable == 0:
            self.current_avail_slice_degraded -= 1

    def sliceDegradedAvailability(self, slice_index):
        if self.k == 1:
            # replication is not affected by this
            return
        unavailable = self.n - self.availableCount(slice_index)
        if unavailable == 0:
            self.current_avail_slice_degraded += 1

    def repair(self, slice_index, repaired_index):
        rc = self.drs_handler.repair(self.status[slice_index], repaired_index)
        if rc < self.drs_handler.RC:
            self.total_optimal_repairs += 1

        return rc * self.conf.chunk_size

    def parallelRepair(self, slice_index, only_lost=False):
        rc = self.drs_handler.parallRepair(self.status[slice_index], only_lost)
        return rc * self.conf.chunk_size

    def isRepairable(self, slice_index):
        return self.drs_handler.isRepairable(self.status[slice_index])

    # corresponding slice is lost or not.
    # True means lost, False means not lost
    def isLost(self, slice_index):
        state = []

        if isinstance(self.status[slice_index], int):
            return True
        for s in self.status[slice_index]:
            if s == 1 or s == 0:
                state.append(1)
            else:
                state.append(s)
        return not self.drs_handler.isRepairable(state)

    def avgTotalSlices(self):
        if not self.scaling:
            return self.total_slices
        else:
            total = 0.0
            for [a, b, c, d] in self.total_slices_table:
                if d == 0:
                    total += (b-a)*c
                else:
                    # f(x) = c + d(x-a) = dx + (c-ad), a<=x<=b
                    total += pow(b-a,2)*d/2 + (c-a*d)*(b-a)
            return round(float(total)/self.end_time, 5)

    # calculate the data unavailable probability
    def calUnavailProb(self):
        total_slices_durations = 0.0
        for item in self.total_slices_table:
            total_slices_durations += item[2] * (item[1] - item[0])
            if item[-1]:
                total_slices_durations += pow(item[1]-item[0], 2) * item[-1]/2

        total_unavailable_durations = 0.0
        slice_indexes = self.unavailable_durations.keys()
        for slice_index in slice_indexes:
            for duration in self.unavailable_durations[slice_index]:
                total_unavailable_durations += duration[1] - duration[0]

        return format(total_unavailable_durations/total_slices_durations, ".4e")

    # "merge_flag=True" means simultaneous multiple unavailable stripes will
    # be treated as one unavailable event;
    # "merge_flag=False" means simultaneous multiple unavailable stripes will be
    # treated as multiple unavailable event.
    def processDuration(self, merge_flag):
        TTFs = []
        # failure timestamps
        FTs = []
        TTRs = []

        unavail_slices = self.unavailable_slice_durations.keys()
        if len(unavail_slices) == 0:
            return [], []

        for slice_index in unavail_slices:
            for duration in self.unavailable_slice_durations[slice_index]:
                if merge_flag and (duration[0] in FTs):
                    continue
                FTs.append(duration[0])
                if len(duration) == 1:
                    TTRs.append(self.end_time - duration[0])
                else:
                    TTRs.append(duration[1] - duration[0])

<<<<<<< HEAD
        FTs.sort()
        TTFs.append(FTs[0])
        for i in xrange(len(FTs)-1):
            TTFs.append(FTs[i+1] - FTs[i])
=======
        unavailability = round(MTTR/(MTTR+MTTF), 5)
>>>>>>> 2fa16837469d31529bc4e75c8624f4dfaf48f046

        return (TTFs, TTRs)

    def calUndurableDetails(self):
        lost_caused_by_LSE = 0
        lost_caused_by_disk = 0
        lost_caused_by_node = 0

        disks_cause_lost = []
        nodes_cause_lost = []

        for slice_index, ts, info in self.undurable_slice_infos:
            component, c_id = info.split(' ')
            if component == "LSE":
                lost_caused_by_LSE += 1
            elif component == "disk":
                lost_caused_by_disk += 1
                if ts not in disks_cause_lost:
                    disks_cause_lost.append(ts)
            elif component == "machine":
                lost_caused_by_node += 1
                if ts not in nodes_cause_lost:
                    nodes_cause_lost.append(ts)
            else:
                raise Exception("Incorrect component")

        return (lost_caused_by_LSE, lost_caused_by_disk, lost_caused_by_node, len(disks_cause_lost), len(nodes_cause_lost))

    # normalized magnitude of data loss, bytes per TB in period of times
    def NOMDL(self, t=None):
        undurable = 0
        if t is None:
            undurable = self.undurable_slice_count
        else:
            for slice_index, ts, info in self.undurable_slice_infos:
                if ts <= t:
                    undurable += 1

        NOMDL = undurable * (self.conf.chunk_size * pow(2, 20)) / (self.conf.total_active_storage * pow(2, 10))
        return NOMDL

    # calculate current total slices to cope with system scaling.
    def calCurrentTotalSlices(self, ts):
        if len(self.total_slices_table) == 1:
            return self.total_slices

        for [s_time, end_time, count, rate] in self.total_slices_table:
            if s_time <= ts <= end_time:
                return int(ceil(count + rate*(ts - s_time)))

    def handleEvent(self, e, queue):
        if e.ignore:
            return
        print "********event info********"
        print "event ID: ", e.event_id
        print "event type: ", e.getType()
        print "event unit: ", e.getUnit().toString()
        print "event Time: ", e.getTime()
        print "event next reovery time: ", e.next_recovery_time

        if e.getType() == Event.EventType.Failure:
            self.handleFailure(e.getUnit(), e.getTime(), e, queue)
        elif e.getType() == Event.EventType.Recovered:
            self.handleRecovery(e.getUnit(), e.getTime(), e, queue)
        elif e.getType() == Event.EventType.LatentDefect:
            self.handleLatentDefect(e.getUnit(), e.getTime(), e)
        elif e.getType() == Event.EventType.LatentRecovered:
            self.handleLatentRecovered(e.getUnit(), e.getTime(), e)
        elif e.getType() == Event.EventType.EagerRecoveryStart:
            self.handleEagerRecoveryStart(e.getUnit(), e.getTime(), e, queue)
        elif e.getType() == Event.EventType.EagerRecoveryInstallment:
            self.handleEagerRecoveryInstallment(e.getUnit(), e.getTime(), e)
        elif e.getType() == Event.EventType.UpgradeCheck:
            self.handleUpgradeCheck(e.getUnit(), e.getTime(), e, queue)
        else:
            raise Exception("Unknown event: " + e.getType())

    def handleFailure(self, u, time, e, queue):
        if e.ignore:
            return

        current_total_slices = self.calCurrentTotalSlices(time)
        if isinstance(u, Machine):
            self.total_machine_failures += 1
            u.setLastFailureTime(e.getTime())

            if e.info == 3:
                self.total_perm_machine_failures += 1
            else:
                if e.info == 1:
                    self.total_short_temp_machine_failures += 1
                elif e.info == 2:
                    self.total_long_temp_machine_failures += 1
                else:
                    self.total_machine_failures_due_to_rack_failures += 1
                    if e.next_recovery_time - e.getTime() <= u.fail_timeout:
                        self.total_short_temp_machine_failures += 1
                    else:
                        self.total_long_temp_machine_failures += 1

            disks = u.getChildren()
            for child in disks:
                slice_indexes = child.getChildren()
                for slice_index in slice_indexes:
                    if slice_index >= current_total_slices:
                        continue
                    if self.status[slice_index] == self.lost_slice:
                        continue

                    if e.info == 3:
                        self.sliceDegraded(slice_index)
                    else:
                        self.sliceDegradedAvailability(slice_index)

                    repairable_before = self.isRepairable(slice_index)
                    index = self.slice_locations[slice_index].index(child)
                    if self.status[slice_index][index] == -1:
                        continue
                    if e.info == 3:
                        self.status[slice_index][index] = -1
                        self._my_assert(self.durableCount(slice_index) >= 0)
                    else:
                        if self.status[slice_index][index] == 1:
                            self.status[slice_index][index] = 0
                        self._my_assert(self.availableCount(slice_index) >= 0)

                    repairable_current = self.isRepairable(slice_index)
                    if repairable_before and not repairable_current:
                        self.unavailable_slice_count += 1
                        if slice_index in self.unavailable_slice_durations.keys():
                            self.unavailable_slice_durations[slice_index].append([time])
                        else:
                            self.unavailable_slice_durations[slice_index] = [[time]]
                        # if one machine failure causes multiple stripes unavailable, or
                        # correlated failures like upgrades result multiple stripes unavailable,
                        # many TTF=0 will be inserted into self.TTFs, decrease MTTF and MTBF
                        # self.current_unavailable_slices[slice_index] = time
                        # self.TTFs.append(time-self.last_failure_ts)
                        # self.last_failure_ts = time

                    if e.info == 3:
                        # lost stripes have been recorded in TTFs.
                        if self.isLost(slice_index):
                            info_logger.info(
                                "time: " + str(time) + " slice:" + str(slice_index) +
                                " durCount:" + str(self.durableCount(slice_index)) +
                                " due to machine " + str(u.getID()))
                            self.status[slice_index] = self.lost_slice
                            self.undurable_slice_count += 1
                            self.undurable_slice_infos.append((slice_index, time, "machine "+ str(u.getID())))
                            continue

        elif isinstance(u, Disk):
            self.total_disk_failures += 1
            u.setLastFailureTime(e.getTime())
            # need to compute projected reovery b/w needed
            projected_bandwidth_need = 0.0

            slice_indexes = u.getChildren()
            for slice_index in slice_indexes:
                if slice_index >= current_total_slices:
                    continue
                if self.status[slice_index] == self.lost_slice:
                    continue

                self.sliceDegraded(slice_index)
                repairable_before = self.isRepairable(slice_index)

                index = self.slice_locations[slice_index].index(u)
                if self.status[slice_index][index] == -1:
                    continue
                self.status[slice_index][index] = -1

                self._my_assert(self.durableCount(slice_index) >= 0)

                repairable_current = self.isRepairable(slice_index)
                if repairable_before and not repairable_current:
                    self.unavailable_slice_count += 1
                    if slice_index in self.unavailable_slice_durations.keys():
                        self.unavailable_slice_durations[slice_index].append([time])
                    else:
                        self.unavailable_slice_durations[slice_index] = [[time]]

                if self.isLost(slice_index):
                    info_logger.info(
                        "time: " + str(time) + " slice:" + str(slice_index) +
                        " durCount:" + str(self.durableCount(slice_index)) +
                        " due to disk " + str(u.getID()))
                    self.status[slice_index] = self.lost_slice
                    self.undurable_slice_count += 1
                    self.undurable_slice_infos.append((slice_index, time, "disk "+ str(u.getID())))
                    continue

        else:
            for child in u.getChildren():
                self.handleFailure(child, time, e, queue)

    def handleRecovery(self, u, time, e, queue):
        if e.ignore:
            return

        current_total_slices = self.calCurrentTotalSlices(time)
        if isinstance(u, Machine):
            self.total_machine_repairs += 1

            # The temporary machine failures is simulated here, while the
            # permanent machine failure is simulated in disk recoveries
            if e.info != 3 and e.info != 4:
                disks = u.getChildren()
                for child in disks:
                    slice_indexes = child.getChildren()
                    for slice_index in slice_indexes:
                        if slice_index >= current_total_slices:
                            continue
                        if self.status[slice_index] == self.lost_slice:
                            if slice_index in self.unavailable_slice_durations.keys() and \
                                len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                                self.unavailable_slice_durations[slice_index][-1].append(time)
                            continue

                        if self.availableCount(slice_index) < self.n:
                            repairable_before = self.isRepairable(slice_index)
                            index = self.slice_locations[slice_index].index(child)
                            if self.status[slice_index][index] == 0:
                                self.status[slice_index][index] = 1
                            self.sliceRecoveredAvailability(slice_index)
                            if not repairable_before and self.isRepairable(slice_index):
                                self.unavailable_slice_durations[slice_index][-1].append(time)
                        elif e.info == 1:  # temp & short failure
                            self.anomalous_available_count += 1
                        else:
                            pass
            elif e.info == 4 or self.conf.queue_disable:  # permanent node failure without queue time
                transfer_required = 0.0
                disks = u.getChildren()
                for disk in disks:
                    indexes = disk.getChildren()
                    for slice_index in indexes:
                        if slice_index >= current_total_slices:
                            continue
                        if self.status[slice_index] == self.lost_slice:
                            if slice_index in self.unavailable_slice_durations.keys() and \
                                len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                                self.unavailable_slice_durations[slice_index][-1].append(time)
                            continue
                        if not self.isRepairable(slice_index):
                            continue

                        threshold_crossed = False
                        actual_threshold = self.recovery_threshold
                        if self.conf.lazy_only_available:
                            actual_threshold = self.n - 1
                        if self.current_slice_degraded < self.conf.max_degraded_slices*current_total_slices:
                            actual_threshold = self.recovery_threshold

                        if self.durableCount(slice_index) <= actual_threshold:
                            threshold_crossed = True

                        if self.availability_counts_for_recovery:
                            if self.availableCount(slice_index) <= actual_threshold:
                                threshold_crossed = True

                        if threshold_crossed:
                            index = self.slice_locations[slice_index].index(disk)
                            if self.status[slice_index][index] == -1 or self.status[slice_index][index] == -2:
                                if self.lazy_recovery or self.parallel_repair:
                                    rc = self.parallelRepair(slice_index)
                                else:
                                    rc = self.repair(slice_index, index)
                                if slice_index in disk.getSlicesHitByLSE():
                                    disk.slices_hit_by_LSE.remove(slice_index)
                                self.total_repairs += 1
                                transfer_required += rc
                                self.total_repair_transfers += rc

                            # must come after all counters are updated
                            self.sliceRecovered(slice_index)
            else:  # e.info == 3 and queue_disable = False,  permanent machine failure with queue time
                chosen_racks = []
                node_repair_time = self.conf.node_repair_time
                node_repair_start = time - node_repair_time
                chosen_racks = self.distributer.getAllRacks()

                if self.conf.data_redundancy[0] in ["MSR", "MBR"]:
                    num = self.conf.drs_handler.d
                else:
                    num = self.conf.drs_handler.k
                recovery_time = self.contention_model.occupy(node_repair_start, chosen_racks, num, node_repair_time)
                recovery_event = Event(Event.EventType.Recovered, recovery_time, u, 4)
                queue.addEvent(recovery_event)
        elif isinstance(u, Disk):
            if e.info != 4 and not self.queue_disable:
                chosen_racks = []
                disk_repair_time = self.conf.disk_repair_time
                disk_repair_start = time - disk_repair_time
                chosen_racks = self.distributer.getAllRacks()

                if self.conf.data_redundancy[0] in ["MSR", "MBR"]:
                    num = self.conf.drs_handler.d
                else:
                    num = self.conf.drs_handler.k
                print "disk contention model start"
                recovery_time = self.contention_model.occupy(disk_repair_start, chosen_racks, num, disk_repair_time)
                print "original recovery time for disk:", time
                print "recovery time for disk:", recovery_time
                recovery_event = Event(Event.EventType.Recovered, recovery_time, u, 4)
                queue.addEvent(recovery_event)
                return

            self.total_disk_repairs += 1

            transfer_required = 0.0
            slice_indexes = u.getChildren()
            for slice_index in slice_indexes:
                if slice_index >= current_total_slices:
                    continue
                if self.status[slice_index] == self.lost_slice:
                    if slice_index in self.unavailable_slice_durations.keys() and \
                        len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                        self.unavailable_slice_durations[slice_index][-1].append(time)
                    continue
                if not self.isRepairable(slice_index):
                    continue

                threshold_crossed = False
                actual_threshold = self.recovery_threshold
                if self.conf.lazy_only_available:
                    actual_threshold = self.n - 1
                if self.current_slice_degraded < self.conf.max_degraded_slices*current_total_slices:
                    actual_threshold = self.recovery_threshold

                if self.durableCount(slice_index) <= actual_threshold:
                    threshold_crossed = True

                if self.availability_counts_for_recovery:
                    if self.availableCount(slice_index) <= actual_threshold:
                        threshold_crossed = True

                if threshold_crossed:
                    index = self.slice_locations[slice_index].index(u)
                    if self.status[slice_index][index] == -1 or self.status[slice_index][index] == -2:

                        if self.lazy_recovery or self.parallel_repair:
                            rc = self.parallelRepair(slice_index)
                        else:
                            rc = self.repair(slice_index, index)
                        if slice_index in u.getSlicesHitByLSE():
                            u.slices_hit_by_LSE.remove(slice_index)
                        self.total_repairs += 1
                        transfer_required += rc
                        self.total_repair_transfers += rc

                    # must come after all counters are updated
                    self.sliceRecovered(slice_index)

        else:
            for child in u.getChildren():
                self.handleRecovery(child, time, e, queue)

    def handleLatentDefect(self, u, time, e):
        current_total_slices = self.calCurrentTotalSlices(time)

        if isinstance(u, Disk):
            slice_count = len(u.getChildren())
            if slice_count == 0:
                return
            self._my_assert(slice_count > 10)

            slice_index = choice(u.getChildren())
            if slice_index >= current_total_slices:
                return

            if self.status[slice_index] == self.lost_slice:
                self.total_skipped_latent += 1
                return

            repairable_before = self.isRepairable(slice_index)

            index = self.slice_locations[slice_index].index(u)
            # A LSE cannot hit lost blocks or a same block multiple times
            if self.status[slice_index][index] == -1 or self.status[slice_index][index] == -2:
                self.total_skipped_latent += 1
                return

            self._my_assert(self.durableCount(slice_index) >= 0)
            self.sliceDegraded(slice_index)

            self.status[slice_index][index] = -2
            u.slices_hit_by_LSE.append(slice_index)
            self.total_latent_failures += 1

            repairable_current = self.isRepairable(slice_index)
            if repairable_before and not repairable_current:
                self.unavailable_slice_count += 1
                if slice_index in self.unavailable_slice_durations.keys():
                    self.unavailable_slice_durations[slice_index].append([time])
                else:
                    self.unavailable_slice_durations[slice_index] = [[time]]
                # self.current_unavailable_slices[slice_index] = time
                # self.TTFs.append(time-self.last_failure_ts)
                # self.last_failure_ts = time
                # self.unavailable_durations.append((time, e.next_recovery_time))

            if self.isLost(slice_index):
                info_logger.info(
                    str(time) + " slice: " + str(slice_index) +
                    " durCount: " + str(self.durableCount(slice_index)) +
                    " latDefect " + str(True) +
                    "  due to ===latent=== error " + " on disk " +
                    str(u.getID()))
                self.undurable_slice_count += 1
                self.undurable_slice_infos.append((slice_index, time, "LSE "+ str(u.getID())))
                self.status[slice_index] = self.lost_slice
        else:
            raise Exception("Latent defect should only happen for disk")

    def handleLatentRecovered(self, u, time, e):
        transfer_required = 0.0
        current_total_slices = self.calCurrentTotalSlices(time)
        if isinstance(u, Disk):
            self.total_scrubs += 1

            slice_indexes = u.getSlicesHitByLSE()
            for slice_index in slice_indexes:
                if slice_index >= current_total_slices:
                    continue
                if self.status[slice_index] == self.lost_slice:
                    if slice_index in self.unavailable_slice_durations.keys() and \
                        len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                        self.unavailable_slice_durations[slice_index][-1].append(time)
                    continue

                if not self.isRepairable(slice_index):
                    continue

                index = self.slice_locations[slice_index].index(u)
                if self.status[slice_index][index] != -2:
                    continue
                self.total_scrub_repairs += 1
                rc = self.repair(slice_index, index)
                u.slices_hit_by_LSE.remove(slice_index)
                self.total_repairs += 1
                transfer_required += rc
                self.total_repair_transfers += rc
                self.sliceRecovered(slice_index)
        else:
            raise Exception("Latent Recovered should only happen for disk")

    def handleUpgradeCheck(self, u, time, e, queue):
        current_total_slices = self.calCurrentTotalSlices(time)

        # 1: check and repair lost chunks which will be offline
        # 2: check and repair unavailable and lost chunks which will be offline
        if e.info == 1 or e.info == 2:
            if not isinstance(u, Machine):
                raise Exception("Check instance is not Machine instance")
            diskes = u.getChildren()
            for disk in diskes:
                slice_indexes = disk.getChildren()
                for slice_index in slice_indexes:
                    if slice_index > current_total_slices:
                        continue
                    if self.status[slice_index] == self.lost_slice:
                        if slice_index in self.unavailable_slice_durations.keys() and \
                            len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                            self.unavailable_slice_durations[slice_index][-1].append(time)
                        continue

                    threshold_crossed = False
                    rc = 0.0
                    index = self.slice_locations[slice_index].index(disk)
                    if self.isRepairable(slice_index):
                        if self.status[slice_index][index] == -1 or self.status[slice_index][index] == -2:
                            threshold_crossed = True
                        if e.info == 2 and self.status[slice_index][index] == 0:
                            threshold_crossed = True
                    if threshold_crossed:
                        rc = self.repair(slice_index, index)
                        if slice_index in disk.getSlicesHitByLSE():
                            disk.slices_hit_by_LSE.remove(slice_index)
                        self.total_repair_transfers += rc
                        self.sliceRecovered(slice_index)
        # 3: check and repair lost chunks on slices which will be offline
        # 4: check and repair unavailable and lost chunks on slices which will be offline
        elif e.info == 3 or e.info == 4:
            if not isinstance(u, Machine):
                raise Exception("Check instance is not Machine instance")
            diskes = u.getChildren()
            for disk in diskes:
                slice_indexes = disk.getChildren()
                for slice_index in slice_indexes:
                    if slice_index > current_total_slices:
                        continue
                    if self.status[slice_index] == self.lost_slice:
                        if slice_index in self.unavailable_slice_durations.keys() and \
                            len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                            self.unavailable_slice_durations[slice_index][-1].append(time)
                        continue

                    threshold_crossed = False
                    rc = 0.0
                    if self.isRepairable(slice_index):
                        if e.info == 3 and self.durableCount(slice_index) < self.n:
                            threshold_crossed = True
                        if e.info == 4 and self.availableCount(slice_index) < self.n:
                            threshold_crossed = True
                    if threshold_crossed:
                        if e.info == 3:
                            rc = self.parallelRepair(slice_index, True)
                        else:
                            rc = self.parallelRepair(slice_index)
                        if slice_index in disk.getSlicesHitByLSE():
                            disk.slices_hit_by_LSE.remove(slice_index)
                        self.total_repair_transfers += rc
                        self.sliceRecovered(slice_index)
        # 5: check and repair all lost slices
        # 6: check and repair all unavailable and lost slices
        elif e.info == 5 or e.info == 6:
            for slice_index, state in enumerate(self.status):
                if slice_index > current_total_slices:
                    continue
                if state == self.lost_slice:
                    if slice_index in self.unavailable_slice_durations.keys() and \
                        len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                        self.unavailable_slice_durations[slice_index][-1].append(time)
                    continue

                threshold_crossed = False
                rc = 0.0
                if self.isRepairable(slice_index):
                    if e.info == 5 and self.durableCount(slice_index) < self.n:
                        threshold_crossed = True
                    if e.info == 6 and self.availableCount(slice_index) < self.n:
                        threshold_crossed = True
                if threshold_crossed:
                    if e.info == 5:
                        rc = self.parallelRepair(slice_index, True)
                    else:
                        rc = self.parallelRepair(slice_index)
                    self.total_repair_transfers += rc
                    self.sliceRecovered(slice_index)
            disks = []
            self.distributer.getAllDisks(u, disks)
            for rack_disks in disks:
                for disk in rack_disks:
                    disk.slices_hit_by_LSE = []
            if time in self.conf.upgrade_ts:
                index = self.conf.upgrade_ts.index(time)
                if index != len(self.conf.upgrade_ts) - 1:
                    regenerate_end_ts = self.conf.upgrade_ts[index+1]
                else:
                    regenerate_end_ts = self.conf.total_time
                timestamps = queue.events.keys()
                timestamps.sort()
                for ts in timestamps:
                    if ts > time:
                        ts_events = queue.events.get(ts)
                        for e in ts_events:
                            if isinstance(e.getUnit(), Disk):
                                queue.events.get(ts).remove(e)
                                if queue.events.get(ts) == []:
                                    queue.events.pop(ts)
                for rack_disks in disks:
                    for disk in rack_disks:
                        if self.conf.failure_generator is not None:
                            disk.failure_generator = self.conf.failure_generator
                        disk.failure_generator.reset(time)
                        disk.last_recovery_time = time
                        if self.conf.lse_generator is not None:
                            disk.latent_error_generator = self.conf.lse_generator
                        disk.latent_error_generator.reset(time)
                        disk.generateEvents(queue, time+1E-5, regenerate_end_ts, True)
        else:
            raise Exception("Incorrect upgrade check style")

    def end(self):
        ret = Result()
        avg_total_slices = self.avgTotalSlices()

        Result.undurable_count = self.undurable_slice_count
        Result.unavailable_count = self.unavailable_slice_count
        # Result.undurable_infos = self.undurable_slice_infos
        Result.undurable_count_details = self.calUndurableDetails()
        Result.unavailable_slice_durations = self.unavailable_slice_durations

        Result.PDL = format(float(self.undurable_slice_count)/avg_total_slices, ".4e")
        Result.NOMDL = self.NOMDL()

        # unavailability from system perspective
        TTFs, TTRs = self.processDuration(True)
        if len(TTFs) == 0 or len(TTRs) == 0:
            Result.MTTR = 0.0
            Result.MTBF = self.end_time
            Result.PUA = 0.0
        else:
            MTTF = sum(TTFs)/len(TTFs)
            MTTR = sum(TTRs)/len(TTRs)
            Result.MTTR = round(MTTR, 4)
            Result.MTBF = round(MTTR + MTTF, 4)
            Result.PUA = format(MTTR/(MTTF+MTTR), ".4e")
        # unavailability from stripe perspective
        TTFs, TTRs = self.processDuration(False)
        Result.PUS = format(sum(TTRs)/(self.end_time * avg_total_slices), ".4e")

        # repair bandwidth in TiBs
        Result.TRT = format(float(self.total_repair_transfers)/pow(2,20), ".4e")

        if not self.queue_disable:
            queue_times, avg_queue_time = self.contention_model.statistics()
            Result.queue_times = queue_times
            Result.avg_queue_time = format(avg_queue_time, ".4f")
            info_logger.info("total times of queuing: %d, average queue time: %f" %
                    (queue_times, avg_queue_time))

        info_logger.info(
            "anomalous available count: %d, total latent failure: %d,\
             total scrubs: %d, total scrubs repairs: %d, \
             total disk failures:%d, total disk repairs:%d, \
             total machine failures:%d, total machine repairs:%d, \
             total permanent machine failures:%d, \
             total short temperary machine failures:%d, \
             total long temperary machine failures:%d, \
             total machine failures due to rack failures:%d, \
             total eager machine repairs:%d, total eager slice repairs:%d, \
             total skipped latent:%d, total incomplete recovery:%d\n \
             max recovery bandwidth:%f\n \
             undurable_slice_count:%d\n \
             total repairs:%d, total optimal repairs:%d" %
            (self.anomalous_available_count, self.total_latent_failures,
             self.total_scrubs, self.total_scrub_repairs,
             self.total_disk_failures, self.total_disk_repairs,
             self.total_machine_failures, self.total_machine_repairs,
             self.total_perm_machine_failures,
             self.total_short_temp_machine_failures,
             self.total_long_temp_machine_failures,
             self.total_machine_failures_due_to_rack_failures,
             self.total_eager_machine_repairs,
             self.total_eager_slice_repairs,
             self.total_skipped_latent,
             self.total_incomplete_recovery_attempts,
             self.max_recovery_bandwidth,
             self.undurable_slice_count,
             self.total_repairs, self.total_optimal_repairs))

        return ret

    def handleEagerRecoveryStart(self, u, time, e, queue):
        self._my_assert(isinstance(u, Machine))
        self.total_eager_machine_repairs += 1
        u.setLastFailureTime(e.getTime())
        original_failure_time = e.getTime()

        # Eager recovery begins now, and ends at time e.next_recovery_time
        # (which is when the machine recovers). Recovery rate will be
        # (recoveryBandwidthCap - currentRecoveryBandwidth) MB/hr. Therefore,
        # total number of chunks that can be recovered = eager recovery
        # duration * recovery rate. This happens in installments, of
        # installmentSize number of chunks each. The last installment will
        # have (total num chunks % installmentSize) number of chunks
        self._my_assert(e.next_recovery_time - e.getTime() > 0)
        self._my_assert(self.current_recovery_bandwidth >= 0)
        recovery_rate = self.recovery_bandwidth_cap - \
            self.current_recovery_bandwidth
        if recovery_rate <= 0:
            return

        num_chunks_to_recover = int((recovery_rate/self.conf.chunk_size) *
                                    (e.next_recovery_time-e.getTime()))
        if num_chunks_to_recover < 1:
            return

        recovery_rate = num_chunks_to_recover*self.conf.chunk_size / \
            (e.next_recovery_time-e.getTime())
        self._my_assert(recovery_rate >= 0)
        self.current_recovery_bandwidth += recovery_rate
        self._my_assert(self.current_recovery_bandwidth >= 0)

        curr_installment_size = self.conf.installment_size
        if num_chunks_to_recover < self.conf.installment_size:
            curr_installment_size = num_chunks_to_recover

        try:
            slice_installment= SliceSet("SliceSet-"+u.toString(), [])
            slice_installment.setLastFailureTime(u.getLastFailureTime())
            slice_installment.setOriginalFailureTime(original_failure_time)
        except Exception, e:
            error_logger.error("Error in eager recovery: " + e)
            return

        total_num_chunks_added_for_repair = 0
        num_chunks_added_to_curr_installment = 0
        curr_time = time
        disks = u.getChildren()
        for child in disks:
            slice_indexes = child.getChildren()
            for slice_index in slice_indexes:
                # When this machine failed, it decremented the availability
                # count of all its slices. This eager recovery is the first
                # point in time that this machine failure has been
                # 'recognized' by the system (since this is when the timeout
                # expires). So if at this point we find any of the
                # availability counts NOT less than n, then we need to count
                # it as an anomaly
                if self.availableCount(slice_index) >= self.n:
                    self.anomalous_available_count += 1
                if self.status[slice_index] == self.lost_slice:
                    continue

                threshold_crossed = False
                actual_threshold = self.recovery_threshold
                expected_recovery_time = curr_time + curr_installment_size * \
                    self.conf.chunk_size/recovery_rate
                actual_threshold = self.conf.getAvailableLazyThreshold(
                    expected_recovery_time -
                    slice_installment.getOriginalFailureTime())

                if self.durableCount(slice_index) <= actual_threshold:
                    threshold_crossed = True

                if self.availability_counts_for_recovery:
                    if self.availableCount(slice_index) <= actual_threshold:
                        threshold_crossed = True

                if threshold_crossed:
                    num_unavailable = self.status[slice_index].count(0)
                    slice_installment.slices.append(slice_index)
                    total_num_chunks_added_for_repair += self.k + \
                        num_unavailable - 1
                    num_chunks_added_to_curr_installment += self.k + \
                        num_unavailable - 1
                    if num_chunks_added_to_curr_installment >= \
                       curr_installment_size - self.k:
                        curr_time += num_chunks_added_to_curr_installment * \
                            self.conf.chunk_size/recovery_rate
                        queue.addEvent(
                            Event(Event.EventType.EagerRecoveryInstallment,
                                  curr_time, slice_installment, False))
                        if total_num_chunks_added_for_repair >= \
                           num_chunks_to_recover - self.k:
                            # the last installment must update recovery
                            # bandwidth
                            slice_installment.setLastBandwidthNeed(
                                recovery_rate)
                            return
                        curr_installment_size = self.conf.installment_size
                        if num_chunks_to_recover - \
                           total_num_chunks_added_for_repair < \
                           self.conf.installment_size:
                            curr_installment_size = num_chunks_to_recover - \
                                total_num_chunks_added_for_repair
                        try:
                            slice_installment = SliceSet("SliceSet-"+u.toString(), [])
                            slice_installment.setLastFailureTime(curr_time)
                            slice_installment.setOriginalFailureTime(
                                original_failure_time)
                            slice_installment.setLastBandwidthNeed(-1)
                        except Exception, e:
                            # error_logger.error("Error in eager recovery: " + e)
                            return
                        num_chunks_added_to_curr_installment = 0

        # Arriving at this point in the code means number of slices added <
        # num_chunks_to_recover
        if len(slice_installment.slices) != 0:
            curr_time += num_chunks_added_to_curr_installment * \
                self.conf.chunk_size/recovery_rate
            slice_installment.setLastBandwidthNeed(recovery_rate)
            queue.addEvent(Event(Event.EventType.EagerRecoveryInstallment,
                                 curr_time, slice_installment, False))
            return

        # No slices were found for eager recovery, undo the current bandwidth
        # need.
        self.current_recovery_bandwidth -= recovery_rate
        self._my_assert(self.current_recovery_bandwidth >= 0)

    def handleEagerRecoveryInstallment(self, u, time, e):
        self._my_assert(isinstance(u, SliceSet))
        transfer_required = 0.0
        if u.getLastBandwidthNeed() != -1:
            self.current_recovery_bandwidth -= u.getLastBandwidthNeed()
            if self.current_recovery_bandwidth < 0 and \
               self.current_recovery_bandwidth > -1:
                self.current_recovery_bandwidth = 0
                self._my_assert(self.current_recovery_bandwidth >= 0)

            for slice_index in u.slices:
                # slice_index = s.intValue()
                if self.status[slice_index] == self.lost_slice:
                    if slice_index in self.unavailable_slice_durations.keys() and \
                        len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                        self.unavailable_slice_durations[slice_index][-1].append(time)
                    continue

                threshold_crossed = False
                actual_threshold = self.recovery_threshold
                # need uc = u?
                actual_threshold = self.conf.getAvailableLazyThreshold(
                    e.getTime() - u.getOriginalFailureTime())

                if self.durableCount(slice_index) <= actual_threshold:
                    threshold_crossed = True

                if self.availability_counts_for_recovery:
                    if self.availableCount(slice_index) <= actual_threshold:
                        threshold_crossed = True

                if threshold_crossed:
                    if self.isLost(slice_index):
                        self.status[slice_index] = self.lost_slice
                        continue
                    if not self.isRepairable(slice_index):
                        continue
                    self.total_eager_slice_repairs += 1
                    if self.lazy_recovery:
                        chunks_recovered = self.parallelRepair(slice_index)
                        # self.handleSliceRecovery(slice_index, e, False)
                        self._my_assert(self.availableCount(slice_index) ==
                                        self.n and
                                        self.durableCount(slice_index) ==
                                        self.n)
                        if self.durableCount(slice_index) != self.n:
                            self.sliceRecovered(slice_index)
                        else:
                            self.sliceRecoveredAvailability(slice_index)
                        transfer_required += self.k - 1 + chunks_recovered
                    else:
                        if self.availableCount(slice_index) < self.n:
                            try:
                                index = self.status[slice_index].index(0)
                            except ValueError:
                                error_logger.error("No block crash in slice " + str(slice_index))
                                continue
                            rc = self.repair(slice_index, index)
                            transfer_required += rc
                            if self.durableCount(slice_index) != self.n:
                                self.sliceRecovered(slice_index)
                            else:
                                self.sliceRecoveredAvailability(slice_index)

            u.setLastFailureTime(e.getTime())

    def handleSliceRecovery(self, slice_index, e, is_durable_failure):
        if self.status[slice_index] == self.lost_slice:
            if slice_index in self.unavailable_slice_durations.keys() and \
                len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                self.unavailable_slice_durations[slice_index][-1].append(e.getTime())
            return 0

        recovered = 0
        if self.availability_counts_for_recovery and \
           (not is_durable_failure or self.lazy_style != LazyStyle.LazyForTransient):
            recovered += self.parallelRepair(slice_index)
        else:
            recovered += self.parallelRepair(slice_index, True)

        self._my_assert(self.durableCount(slice_index) == self.n)
        return recovered

if __name__ == "__main__":
    print "hello"
