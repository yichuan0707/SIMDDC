import ConfigParser
import os
from math import ceil, floor
from random import random
from simulator.utils import splitMethod, splitIntMethod, splitFloatMethod, extractDRS
from simulator.Log import info_logger
from simulator.drs.Handler import getDRSHandler

BASE_PATH = r"/root/SIMDDC/"
CONF_PATH = BASE_PATH + "conf/"


def getConfParser(conf_file_path):
    conf = ConfigParser.ConfigParser()
    conf.read(conf_file_path)
    return conf


class Configuration(object):
    path = CONF_PATH + "simddc.conf"

    def __init__(self, path=None):
        if path is None:
            conf_path = Configuration.path
        else:
            conf_path = path
        self.conf = getConfParser(conf_path)

        try:
            d = self.conf.defaults()
        except ConfigParser.NoSectionError:
            raise Exception("No Default Section!")

        self.total_time = int(d["total_time"])
        # total active storage in PBs
        self.total_active_storage = float(d["total_active_storage"])

        self.chunk_size = int(d["chunk_size"])
        self.disk_capacity = float(d["disk_capacity"])
        # tanslate TB of manufacturrer(10^12 bytes) into GBs(2^30 bytes)
        self.actual_disk_capacity = self.disk_capacity * pow(10, 12)/ pow(2, 30)
        self.max_chunks_per_disk = floor(self.actual_disk_capacity*1024/self.chunk_size)
        self.disks_per_machine = int(d["disks_per_machine"])
        self.machines_per_rack = int(d["machines_per_rack"])
        self.rack_count = int(d["rack_count"])
        self.datacenters = 1

        self.event_file = d.pop("event_file", None)

        # If n <= 15 in each stripe, no two chunks are on the same rack.
        self.num_chunks_diff_racks = 15

        self.data_redundancy = d["data_redundancy"]
        data_redundancy = extractDRS(self.data_redundancy)

        self.lazy_recovery = self._bool(d.pop("lazy_recovery", "false"))
        self.lazy_only_available = self._bool(d.pop("lazy_only_available", "true"))

        self.recovery_bandwidth_cross_rack = int(d["recovery_bandwidth_cross_rack"])
        self.queue_disable = self._bool(d.pop("queue_disable", "true"))
        self.bandwidth_contention = d["bandwidth_contention"]
        self.node_bandwidth = int(d["node_bandwidth"])

        self.parallel_repair = self._bool(d.pop("parallel_repair", "false"))

        self.availability_counts_for_recovery = self._bool(d[
            "availability_counts_for_recovery"])

        self.availability_to_durability_threshold = splitIntMethod(d["availability_to_durability_threshold"])
        self.recovery_probability = splitIntMethod(d["recovery_probability"])
        self.max_degraded_slices = float(d["max_degraded_slices"])
        self.installment_size = int(d["installment_size"])

        self.outputs = splitMethod(d["outputs"])

        self.drs_handler = getDRSHandler(data_redundancy[0], data_redundancy[1:])
        if not self.lazy_recovery:
            self.recovery_threshold = self.drs_handler.n - 1
        else:
            self.recovery_threshold = int(d.pop("recovery_threshold"))

        self.system_scaling = self._bool(d.pop("system_scaling", "false"))
        if self.system_scaling:
            sections = self._getSections("System Scaling")
            if sections == []:
                raise Exception("No System Scaling section in configuration file")
            self.system_scaling_infos = []
            for section in sections:
                self.system_scaling_infos.append(self.parserScalingSettings(section))

        self.system_upgrade = self._bool(d.pop("system_upgrade", "false"))
        if self.system_upgrade:
            sections = self._getSections("System Upgrade")
            if sections == []:
                raise Exception("No System Upgrade section in configuration file")
            self.system_upgrade_infos = []
            for section in sections:
                self.system_upgrade_infos.append(self.parserUpgradeSettings(section))

        self.correlated_failures = self._bool(d.pop("correlated_failures", "false"))
        if self.correlated_failures:
            sections = self._getSections("Correlated Failures")
            if sections == []:
                raise Exception("No Correlated Failures section in configuration file")
            self.correlated_failures_infos = []
            for section in sections:
                self.correlated_failures_infos.append(self.parserCorrelatedSetting(section))

        self.disk_repair_time, self.node_repair_time = self.comRepairTime()

        self.total_slices = int(ceil(self.total_active_storage*pow(2,30)/(self.drs_handler.k*self.chunk_size)))

    def _bool(self, string):
        if string.lower() == "true":
            return True
        elif string.lower() == "false":
            return False
        else:
            raise Exception("String must be 'true' or 'false'!")

    def _checkSpace(self):
        total_system_capacity = self.actual_disk_capacity * self.disks_per_machine * self.machines_per_rack * self.rack_count
        min_necess_capacity = float(self.total_active_storage) * self.drs_handler.n / self.drs_handler.k
        if min_necess_capacity >= total_system_capacity:
            raise Exception("Not have enough space!")

    def _getSections(self, section_start):
        sections = []
        all_sections = self.conf.sections()
        for item in all_sections:
            if item.startswith(section_start):
                sections.append(item)
        return sections

    def getDRSHandler(self):
        return self.drs_handler

    def comRepairTime(self):
        repair_traffic = self.drs_handler.repairTraffic()
        # in MB/s
        aggregate_bandwidth = self.recovery_bandwidth_cross_rack * self.rack_count

        # used disk space in MBs
        used_disk_space = self.drs_handler.SO*self.total_active_storage*pow(2,30)/(self.rack_count*self.machines_per_rack*self.disks_per_machine)

        # repair time in hours
        disk_repair_time = round(repair_traffic*used_disk_space/aggregate_bandwidth, 5)
        node_repair_time = disk_repair_time*self.disks_per_machine

        return disk_repair_time, node_repair_time

    # "True" means events record to file, and vice versa.
    def eventToFile(self):
        return self.event_file is not None

    def parserScalingSettings(self, section_name):
        scaling_start = self.conf.getint(section_name, "scaling_start")
        style = self.conf.getint(section_name, "style")
        inc_capacity = self.conf.getfloat(section_name, "inc_capacity")
        inc_slices = self.conf.getint(section_name, "inc_slices")
        add_slice_start = self.conf.getint(section_name, "add_slice_start")
        slice_rate = self.conf.getfloat(section_name, "slice_rate")
        load_balancing = self.conf.getboolean(section_name, "load_balancing")

        new_disk_capacity = None
        if style == 0:
            new_disk_capacity = self.conf.getfloat(section_name, "new_disk_capacity")
        try:
            disk_failure_generator = self.conf.get(section_name, "disk_failure_generator")
        except ConfigParser.NoOptionError:
            disk_failure_generator = None
        try:
            disk_recovery_generator = self.conf.get(section_name, "disk_recovery_generator")
        except ConfigParser.NoOptionError:
            disk_recovery_generator = None

        return [scaling_start, style, inc_capacity, inc_slices, add_slice_start, slice_rate, load_balancing,
                new_disk_capacity, disk_failure_generator, disk_recovery_generator]

    def parserUpgradeSettings(self, section_name):
        style = self.conf.getint(section_name, "style")
        domain = self.conf.get(section_name, "domain")
        freq = self.conf.getint(section_name, "freq")
        interval = self.conf.getfloat(section_name, "interval")
        downtime = self.conf.getfloat(section_name, "downtime")

        return (style, domain, freq, interval, downtime)

    def parserCorrelatedSetting(self, section_name):
        occurrence_timestamp = self.conf.getint(section_name, "occurrence_timestamp")
        una_scope = self.conf.get(section_name, "una_scope")
        una_downtime = self.conf.getfloat(section_name, "una_downtime")
        try:
            dl_scope = self.conf.get(section_name, "dl_scope")
            dl_downtime = self.conf.getfloat(section_name, "dl_downtime")
        except ConfigParser.NoOptionError:
            dl_scope = None
            dl_downtime = None
        try:
            choose_from_una = self.conf.getboolean(section_name, "choose_from_una")
        except ConfigParser.NoOptionError:
            choose_from_una = False

        return (occurrence_timestamp, una_scope, una_downtime, dl_scope, dl_downtime, choose_from_una)

    # Time table for total slices changes
    def tableForTotalSlice(self):
        time_table = []
        total_slices = self.total_slices
        if not self.system_scaling:
            time_table.append([0, self.total_time, total_slices, 0])
        else:
            start_time = 0
            end_time = 0
            for i, info in enumerate(self.system_scaling_infos):
                end_time = info[0] + info[4]
                time_table.append([start_time, end_time, total_slices, 0])
                start_time = end_time
                end_time += float(info[3])/info[5]
                time_table.append([start_time, end_time, total_slices, info[5]])
                total_slices += info[3]

            time_table.append([end_time, self.total_time, total_slices, 0])

        return time_table

    def getAvailableLazyThreshold(self, time_since_failed):
        threshold_gap = self.drs_handler.n - 1 - self.recovery_threshold
        length = len(self.availability_to_durability_threshold)
        index = 0
        for i in xrange(length-1):
            if self.availability_to_durability_threshold[i] < \
               time_since_failed and \
               self.availability_to_durability_threshold[i+1] >= \
               time_since_failed:
                if i > 0:
                    index = i
                break
        threshold_increment = threshold_gap * \
            (1 if random() < self.recovery_probability[i] else 0)
        return self.recovery_threshold + threshold_increment


    def returnSliceSize(self):
        return self.chunk_size * self.drs_handler.n

    def returnAll(self):
        d = {"total_time": self.total_time,
             "total_active_storage": self.total_active_storage,
             "chunk_size": self.chunk_size,
             "disk_capacity": self.disk_capacity,
             "disks_per_machine": self.disks_per_machine,
             "machines_per_rack": self.machines_per_rack,
             "event_file": self.event_file,
             "recovery_threshold": self.recovery_threshold,
             "lazy_only_available": self.lazy_only_available,
             "data_redundancy": self.data_redundancy,
             "outputs": self.outputs,
             "recovery_bandwidth_cross_rack": self.recovery_bandwidth_cross_rack,
             "availability_counts_for_recovery":
             self.availability_counts_for_recovery,
             "parallel_repair": self.parallel_repair,
             "lazy_recovery": self.lazy_recovery,
             "system_scaling": self.system_scaling,
             "system_upgrade": self.system_upgrade,
             "correlated_failures": self.correlated_failures}

        if self.system_scaling:
            d["system_scaling_infos"] = self.system_scaling_infos
        if self.system_upgrade:
            d["system_upgrade_infos"] = self.system_upgrade_infos
        if self.correlated_failures:
            d["correlated_failures"] = self.correlated_failures_infos

        return d

    def printTest(self):
        d = self.returnAll()
        keys = d.keys()
        for key in keys:
            print key, d[key]

    def printAll(self):
        default_infos = "Default Configurations: \t total_time: " + str(self.total_time) + \
                        ", disk capacity: " + str(self.disk_capacity) + "TB" + \
                        ", disks per machine: " + str(self.disks_per_machine) + \
                        ", machines per rack: " + str(self.machines_per_rack) + \
                        ", rack count: " + str(self.rack_count) + \
                        ", chunk size: " + str(self.chunk_size) + "MB" + \
                        ", total active storage: " + str(self.total_active_storage) + "PB" +\
                        ", data redundancy: " + str(self.data_redundancy) + \
                        ", recovery bandwidth cross rack: " + str(self.recovery_bandwidth_cross_rack) + \
                        ", event file path: " + self.event_file + \
                        ", outputs: " + str(self.outputs) + \
                        ", parallel repair: " + str(self.parallel_repair) + \
                        ", system Scaling flag: " + str(self.system_scaling) + \
                        ", system upgrade flag: " + str(self.system_upgrade) + \
                        ", correlated failures flag: " + str(self.correlated_failures)

        info_logger.info(default_infos)

        if self.system_scaling:
            info_logger.info("System scaling Configurations: " + str(self.system_scaling_infos))

        if self.system_upgrade:
            info_logger.info("System upgrade format:(style, domain, freq, interval, downtime)")
            info_logger.info("System upgrade Configurations: " + str(self.system_upgrade_infos))

        if self.correlated_failures:
            info_logger.info("Correlated Failures Configurations: " + str(self.correlated_failures_infos))


if __name__ == "__main__":
    conf = Configuration("/root/SIMDDC/conf/samples/upgrade-simddc.conf")
    drs_handler = conf.getDRSHandler()
    print conf.tableForTotalSlice()
    print conf.total_slices
    print conf.rack_count
    conf.printTest()
    conf.printAll()
    print conf.disk_repair_time, conf.node_repair_time
