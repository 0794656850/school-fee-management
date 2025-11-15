from utils.report_scheduler import schedule_reports
from apscheduler.schedulers.background import BackgroundScheduler

def enable_reports_scheduler(app):
    try:
        sched = BackgroundScheduler()
        # keep any existing jobs from scheduler.py if you use it elsewhere
        schedule_reports(sched, app)
        sched.start()
        return sched
    except Exception as e:
        print('[scheduler] failed to start reports scheduler:', e)
        return None
