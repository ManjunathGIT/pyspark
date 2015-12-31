import os
import commands

user = os.getlogin()

groups = commands.getoutput("groups " + user)

teams = {"datacubic": "datacubic_conf", "topweibo": "topweibo_conf"}

for team in teams:
    if team in groups:
        print teams[team]

        break
