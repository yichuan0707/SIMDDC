import os
import sys
import csv

from random import uniform, sample
from copy import deepcopy
from time import strftime

from simulator.Event import Event
from simulator.Result import Result
from simulator.utils import splitMethod
from simulator.EventQueue import EventQueue
from simulator.Log import info_logger, error_logger
from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser

from simulator.unit.Rack import Rack
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk

from simulator.eventHandler.EventHandler import EventHandler
from simulator.dataDistribute.SSSDistribute import SSSDistribute

DEFAULT = r"/root/SIMDDC/conf/"
RESULT = r"/root/SIMDDC/log/"


class Simulation(object):

    def __init__(self, conf_path):
        self.conf_path = conf_path

        self.scaling_intervals = []
        self.iteration_times = 1
        self.ts = strftime("%Y%m%d.%H.%M.%S")
        self.total_events_handled = 0

    def _failedComponents(self, info, interval):
        info.strip()
        scope = splitMethod(info, '_')
        failure_components = []
        failed_amounts = []

        length = len(scope)/2
        for i in xrange(length):
            failed_amounts.append(scope[2*i:2*(i+1)])

        # racks come from system tree, so we can not modify it.
        racks = self.distributer.getAllRacks()
        for num, component in failed_amounts:
            if component == "rack":
                failed_racks = sample(racks, int(num))
                for rack in failed_racks:
                    rack.addFailureInterval(interval)
                    failure_components.append(rack)
            elif component == "machine":
                machines = []
                for rack in racks:
                    if rack in failure_components:
                        continue
                    else:
                        machines += rack.getChildren()
                failed_machines = sample(machines, int(num))
                for machine in failed_machines:
                    machines.remove(machine)
                    machine.addFailureInterval(interval)
                    failure_components.append(machine)
            elif component == "disk":
                disks = []
                for rack in racks:
                    if rack in failure_components:
                        continue
                    rack_disks = []
                    self.distributer.getAllDisksInRack(rack, rack_disks)
                    disks += rack_disks
                failed_disks = sample(disks, int(num))
                for disk in failed_disks:
                    disk.addFailureInterval(interval)
                    failure_components.append(disk)

        return failure_components

    def getDistributer(self):
        return self.distributer

    def addCorrelatedFailures(self, cf_info):
        choose_from_una = cf_info[-1]

        # interval formats: [failure start time, failure end time, lost flag]
        #                   lost flag = False, unavailable event, default
        #                   lost flag = True, lost event.
        una_interval = [cf_info[0], cf_info[2] + cf_info[0], False]
        una_components = self._failedComponents(cf_info[1], una_interval)

        if cf_info[3] != None:
            dl_interval = [cf_info[0], cf_info[4] + cf_info[0], True]
            if cf_info[3] == cf_info[1]:
                for component in una_components:
                    component.addFailureInterval(dl_interval)
            else:
                if choose_from_una:
                    failed_num = splitMethod(cf_info[3], '_')
                    components_for_choosen = []
                    if failed_num[1] == "machine":
                        for component in una_components:
                            if isinstance(component, Rack):
                                for child in component.getChildren():
                                    components_for_choosen.append(child)
                            if isinstance(component, Machine):
                                components_for_choosen.append(component)
                        dl_components = sample(components_for_choosen, int(failed_num[0]))
                    elif failed_num[1] == "disk":
                        for component in una_components:
                            if isinstance(component, Rack):
                                disks = []
                                self.distributer.getAllDisksInRack(component, disks)
                                for disk in disks:
                                    components_for_choosen.append(disk)
                            if isinstance(component, Machine):
                                for disk in component.getChildren():
                                    components_for_choosen.append(disk)
                            if isinstance(component, Disk):
                                components_for_choosen.append(disk)
                        dl_components = sample(components_for_choosen, int(failed_num[0]))
                    else:
                        raise Exception("Unsupport lost event!")
                    for component in dl_components:
                        component.addFailureInterval(dl_interval)
                else:
                    # here, unavailable compoent should be excluded.
                    # But, a little complicate to implement.
                    dl_components = self._failedComponents(cf_info[3], dl_interval)

                for component in dl_components:
                    if (component in una_components) or (component.getParent() in una_components) or (component.getParent().getParent() in una_components):
                        component.updateFailureInterval(dl_interval, cf_info[2] + cf_info[0])

        return self.distributer.getRoot()

    def addSystemUpgrade(self, upgrade_info, end_time):
        style = upgrade_info[0]
        domain_infos = splitMethod(upgrade_info[1], '_')
        freq = upgrade_info[2]
        interval = upgrade_info[3]
        downtime = upgrade_info[4]

        upgrade_start = round(uniform(0, freq), 3)

        upgrade_times = int((end_time - upgrade_start)/freq)
        for j in xrange(upgrade_times+1):
            system_upgrade_start = j*freq + upgrade_start

            if domain_infos[1] == "rack":
                upgrade_domain_in_racks = int(domain_infos[0])
                racks = self.distributer.getAllRacks()
                loops = len(racks)/upgrade_domain_in_racks
                remainder = len(racks) % upgrade_domain_in_racks
                for i in xrange(loops):
                    for rack in racks[i*upgrade_domain_in_racks:(i+1)*upgrade_domain_in_racks]:
                        machines = rack.getChildren()
                        for machine in machines:
                            start_time = system_upgrade_start + (downtime + interval)*i
                            machine.addFailureInterval([start_time, start_time + downtime, False])
                if remainder != 0:
                    for rack in racks[-remainder:]:
                        machines = rack.getChildren()
                        for machine in machines:
                            start_time = system_upgrade_start + (downtime + interval) * loop_times
                            machine.addFailureInterval([start_time, start_time + downtime, False])
            else:
                pass

    def addSystemScaling(self, scale_info):
        e_generators = {}
        if scale_info[-1] != None or scale_info[-2] != None:
            e_generators["d_generators"] = scale_info[-2:]


        start_time = scale_info[0] + scale_info[4]
        end_time = start_time + float(scale_info[3])/scale_info[5]
        # interval formats: start_time, end_time, rate, style
        self.scaling_intervals.append((start_time, end_time, scale_info[5], scale_info[1]))
        self.distributer.systemScaling(scale_info[0], scale_info[2], scale_info[3], scale_info[1],
                                       scale_info[7], scale_info[6], {})

    def writeToCSV(self, res_file_path, contents):
        with open(res_file_path, "w") as fp:
            writer = csv.writer(fp, lineterminator='\n')
            for item in contents:
                writer.writerow(item)

    def run(self):
        conf = Configuration(self.conf_path)
        xml = XMLParser(conf)
        self.distributer = SSSDistribute(xml)
        self.conf = self.distributer.returnConf()

        self.event_handler = EventHandler
        self.distributer.start()

        if self.conf.system_upgrade:
            for info in self.conf.system_upgrade_infos:
                self.addSystemUpgrade(info, self.conf.total_time)
        if self.conf.correlated_failures:
            for info in self.conf.correlated_failures_infos:
                for i in xrange(10):
                    cf_info = deepcopy(list(info))
                    cf_info[0] += i * 8760
                    print "correlated_failures info:", cf_info
                    self.addCorrelatedFailures(cf_info)
        if self.conf.system_scaling:
            for info in self.conf.system_scaling_infos:
                self.addSystemScaling(info)

        info_logger.info("disk usage is: " + str(self.distributer.diskUsage()*100) + "%\n")
        self.distributer.getRoot().printAll()

        events_handled = 0
        events = EventQueue()

        root = self.distributer.getRoot()
        root.generateEvents(events, 0, self.conf.total_time, True)

        # if False:
        if self.conf.event_file != None:
            events_file = self.conf.event_file + '-' + self.ts
            events.printAll(events_file, "Iteration number: "+str(self.iteration_times))
        self.iteration_times += 1

        handler = self.event_handler(self.distributer)

        print "total slices:", handler.total_slices
        e = events.removeFirst()
        while e is not None:
            handler.handleEvent(e, events)
            e = events.removeFirst()
            events_handled += 1

        self.total_events_handled += events_handled

        result = handler.end()
        info_logger.info(result.toString())
        return result

    def main(self, num_iterations):
        contents = []

        for i in xrange(num_iterations):
            result = self.run()
            contents.append([result.data_loss_prob, result.unavailable_prob, result.total_repair_transfers] +
                            list(result.undurable_count_details) + [str(result.queue_times)+'*'+str(result.avg_queue_time)])

        res_file_path = RESULT + self.conf.data_redundancy + '-'
        if self.conf.system_upgrade:
            res_file_path += 'upgrade-'
        res_file_path += self.ts + ".csv"
        self.writeToCSV(res_file_path, contents)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise Exception("Usage: python Test.py conf_path num_iterations")
    path = sys.argv[1]
    num_iterations = int(sys.argv[2])

    if not os.path.isabs(path):
        conf_path = DEFAULT + path
    else:
        conf_path = path

    sim = Simulation(conf_path)
    sim.main(num_iterations)
