

class Result(object):
    undurable_count = 0
    unavailable_count = 0
    # undurable caused by LSE, disk, node, disk count cause lost, node count cause lost
    undurable_count_details = None
    unavailable_slice_durations = {}

    # PDL = (lost slices)/(total slices)
    PDL = 0.0
    NOMDL = 0

    # mean-time-to-repair, from system level
    MTTR = 0.0
    # mean-time-between-failure, MTBF = MTTR + MTTF, from system level
    MTBF = 0.0
    # PUA = MTTR/MTBF
    PUA = 0.0
    # PUS = (all slices' failure durations)/(total slices * mission time), from slice level
    PUS = 0.0

    # total repair traffic
    TRT = 0.0

    queue_times = 0
    avg_queue_time = 0.0

    def toString(self):
        return "unavailable=" + str(Result.unavailable_count) + \
            "; undurable=" + str(Result.undurable_count) + \
            "; PDL=" + str(Result.PDL) + \
            "; NOMDL=" + str(Result.NOMDL) + \
            "; MTTR=" + str(Result.MTTR) + \
            "; MTBF=" + str(Result.MTBF) + \
            "; PUA=" + str(Result.PUA) + \
            "; PUS=" + str(Result.PUS) + \
            "; TRT=" + str(Result.TRT) + "TiB" + \
            "; queue times=" + str(Result.queue_times) + \
            "; average queue time=" + str(Result.avg_queue_time) + "h."


if __name__ == "__main__":
    r = Result()
    print r.toString()
