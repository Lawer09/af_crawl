import stores_m
from utils import timeGen

if __name__ == "__main__":
    stores_m.user_apps_tasks.export_fail_tasks(f"{timeGen.get_now_str()}.csv")