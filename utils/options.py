import sys


def get_index():
    if len(sys.argv) > 1:
        return int(sys.argv[1])
    return -1

def get_day():
    if len(sys.argv) > 2:
        return int(sys.argv[2])
    return 1