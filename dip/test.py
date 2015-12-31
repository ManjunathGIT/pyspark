import os
import commands

user = os.getlogin()

groups = commands.getoutput("groups " + user)

print groups

teams = {"datacubic": "datacubic_conf", "topweibo": "topweibo_conf"}

for team in teams:
	print team
	
    if team in groups:
        print teams[team]

        break
