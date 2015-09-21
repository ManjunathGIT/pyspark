from datetime import datetime, timedelta

scheduleTime = datetime.strptime("20150921172839", "%Y%m%d%H%M%S")

preHour = scheduleTime - timedelta(hours=1)

preDay = scheduleTime - timedelta(days=1)

print "preHour:", preHour.strftime("%Y%m%d%H")

print "preDay:", preDay.strftime("%Y%m%d")
