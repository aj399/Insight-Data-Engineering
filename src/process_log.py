#PYTHON 2.7
from __future__ import division
import operator
import re
import sys
from collections import defaultdict
import datetime
from collections import OrderedDict

def reqFeatExt(line, hostPattern, timePattern, reqPatSub, reqPattern, statPattern):
  
  hostGrp = hostPattern.match(line)
  host = hostGrp.group(0)[:-1]
  timeGrp = timePattern.search(line)
  timeStr = timeGrp.group(0)[1:-1]
  Time = datetime.datetime.strptime(timeStr[:-6], '%d/%b/%Y:%H:%M:%S')
  reqSubGrp = reqPatSub.search(line)
  reqGrp = reqPattern.search(reqSubGrp.group(0))
  if reqGrp == None:
    req = "unknown"
  else:
    req = reqGrp.group(0)
  req = req[:-1]
  statGrp = statPattern.search(line)
  stat = statGrp == None
  sByte = line.rsplit(None, 1)[-1]
  bytes = 0
  if sByte != '-':
    bytes = int(sByte)
  return host, Time, req, stat, bytes

def top(topElt, Elt, topNum):
  
  for index, elt in enumerate(topElt):
  
    if Elt[0] == elt[0]:
    
      del topElt[index]
      break
    
    
  index = 0
  flag = True
  for elt in topElt:
  
    if Elt[1] > elt[1]:
    
      topElt.insert(index, Elt)
      flag = False
      break
    elif Elt[1] == elt[1]:
      
      if Elt[0] < elt[0]:
        
        topElt.insert(index, Elt)
        flag = False
        break
      
      
    index += 1
    
  if flag:
    
    topElt.insert(index, Elt)
      
  if len(topElt) > topNum:
    
    del topElt[-1]
    
  return topElt
  
def timeEval(topTimes, Time, times):

  if(not times):
    
    times[Time] = 0
  else:
  
    stTime = next(reversed(times))+ datetime.timedelta(0,1)
    while (Time-stTime).total_seconds()>=0.0:
      
      times[stTime] = 0
      stTime = stTime + datetime.timedelta(0,1)
    
  for key, value in times.iteritems():
    
    if (Time-key).total_seconds()>3600:
    
      del times[key]
      topTimes = top(topTimes, [key, value], 10)
    
    else:
      
      times[key] = times[key]+1
    
  
  return topTimes, times

def firewallEmul(line, Time, host, blocked, blist, blockSites):

  for key in blockSites.keys():
    
    value = blockSites[key]
    if (Time-value).total_seconds() > 300.0:
    
      del blockSites[key]
      
  
  for key in blist.keys():
    
    values = blist[key]
    for i, value in enumerate(values):
    
      if (value-Time).total_seconds() > 20.0:
    
        del values[i]
      
    
    if not values:
    
      del blist[key]

  if host in blockSites:
  
    blocked.append(line)
      
  if host not in blist:
  
    blist[host] = [Time]
    
  else:
  
    if len(blist[host])>1:
    
      blockSites[host] = Time
      del blist[host][0]
      
    else:
      
      blist[host].append(Time)

  
  return blocked, blist, blockSites
  
if __name__ == '__main__':  
  
  if len(sys.argv)<5:
    print "Wrong No: of Input Parameters"
    print "Required format:"
    print "Argument 1: Path to log file"
    print "Argument 2: Path to hosts output file"
    print "Argument 3: Path to hours output file"
    print "Argument 4: Path to resources output file"
    print "Argument 5: Path to blocked output file"
    
  logFile = sys.argv[1]
  hostsFile = sys.argv[2]
  hoursFile = sys.argv[3]
  resourcesFile = sys.argv[4]
  blockedFile = sys.argv[5]
  fLogFile = open(logFile,'r')
  fHostsFile = open(hostsFile, 'w')
  fHoursFile = open(hoursFile, 'w')
  fresourcesFile = open(resourcesFile, 'w')
  fBlockedFile = open(blockedFile, 'w')
  
  hostPattern = re.compile('.*?\s')
  timePattern = re.compile('\[.*?\]')
  reqPatSub = re.compile('\".*\"')
  reqPattern = re.compile('/.*?(\s|\")')
  statPattern = re.compile('HTTP/1.0\" 401')
  
  dictTemp = {}
  hosts = defaultdict(lambda: 0, dictTemp)
  topHosts = []
  requests = defaultdict(lambda: 0, dictTemp)
  requestNum = defaultdict(lambda: 0, dictTemp)
  topRequests = []
  times = OrderedDict()
  topTimes = []
  blocked = []
  blist = {}
  blockSites = {}
  for line in fLogFile:
    
    host, Time, request, status, bytes  = reqFeatExt(line, hostPattern, timePattern, reqPatSub, reqPattern, statPattern)
    hosts[host] += 1
    curHost = [host, hosts[host]]
    topHosts = top(topHosts, curHost, 10)
    requestNum[request] += bytes
    curRequest = [request, requests[request]]
    topRequests = top(topRequests, curRequest, 10)
    topTimes, times = timeEval(topTimes, Time, times)
    if not status:

      blocked, blist, blockSites = firewallEmul(line, Time, host, blocked, blist, blockSites)
  
  
  for key, value in times.iteritems():
  
    topTimes = top(topTimes, [key, value], 10)  
    
  for Host in topHosts:
    
    fHostsFile.write(str(Host[0])+","+str(Host[1])+"\n")
    
    
  for Requests in topRequests:
  
    fresourcesFile.write(Requests[0]+"\n")
  
  
  for Times in topTimes:
  
    fHoursFile.write(Times[0].strftime('%d/%b/%Y:%H:%M:%S')+" -0400,"+str(Times[1])+"\n")
  
  for block in blocked:
  
    fBlockedFile.write(block)
  
  fLogFile.close()
  fHostsFile.close()
  fHoursFile.close()
  fresourcesFile.close()
  fBlockedFile.close()