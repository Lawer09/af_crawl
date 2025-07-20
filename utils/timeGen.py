from datetime import datetime, timedelta
from typing import Union
import setting

def get_last_N_days(today=datetime.now().date(), n=30):
    # 初始化日期列表
    last_N_days = []
    
    # 循环获取最近N天的日期
    for i in range(n):
        # 计算日期
        date = today - timedelta(days=i)
        
        # 将日期添加到列表中
        last_N_days.append(date.strftime('%Y-%m-%d'))
    
    # 返回日期列表
    return last_N_days

def get_now_str():
    return get_now().strftime('%Y-%m-%d %H:%M:%S')

def get_now_date():
    return get_now().strftime('%Y-%m-%d')

def get_now_date_with_give_clock_time(clock_time_str):
    return get_now().strftime('%Y-%m-%d') + ' ' + clock_time_str

def get_now():
    return datetime.now(setting.TIMEZONE)

def get_date_from_str(date_str: str):
    return datetime.strptime(date_str, '%Y-%m-%d')

def after_now(days=0, hours=0, minutes=0, seconds=0) -> str:
    return after(get_now(), days=days, hours=hours, minutes=minutes, seconds=seconds)

def after(date: Union[datetime, str], days=0, hours=0, minutes=0, seconds=0) -> str:
    """在指定时间基础上偏移，返回格式为 'YYYY-MM-DD HH:MM:SS'"""
    if isinstance(date, str):
        date = datetime.fromisoformat(date)
    dt = date + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def is_before_today_time(target_time_str: str) -> bool:
    """
    判断当前时间是否在今天指定时间点之前
    """
    try:
        now = get_now()  # aware datetime
        target_time = datetime.strptime(target_time_str, "%H:%M:%S").time()
        # 创建带时区的目标时间
        today_target = datetime.combine(now.date(), target_time).replace(tzinfo=setting.TIMEZONE)
        return now < today_target
    except Exception as e:
        print(f"[TIME ERROR] 无法判断时间：{e}")
        return False

def seconds_between(start: Union[str, datetime], end: Union[str, datetime]) -> int:
    """返回两个时间相差的秒数"""
    if isinstance(start, str):
        start = datetime.fromisoformat(start)
    if isinstance(end, str):
        end = datetime.fromisoformat(end)
    return int((end - start).total_seconds())

if __name__ == '__main__':
    strs = get_now_str()
    print(strs)
    print(after_now(seconds=1200))