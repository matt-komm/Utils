#!/usr/bin/env python

import argparse
import hashlib
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser()
parser.add_argument('reference', type=str, nargs=2)
args = parser.parse_args()

def parseFrame(frame):
    ip = frame.find("ip")
    obj = frame.find("obj")
    fn = frame.find("fn")
    lno = frame.find("line")
    filePath = frame.find("file")
    if ip is not None and obj is not None and fn is not None and (lno is None or filePath is None):
        return {"ip":ip.text,"obj":obj.text,"fn":fn.text}
    if ip is not None and obj is not None and fn is not None and lno is not None or filePath is not None:
        return {"ip":ip.text,"obj":obj.text,"fn":fn.text,"lno":lno.text,"file":filePath.text}
    return None

def parseStack(stack):
    frames = []
    localObj = False
    stackHash = 0x9e3779b9
    for frame in stack.iterfind("frame"):
        parsedFrame = parseFrame(frame)
        if parsedFrame:
            stackHash ^= hash(parsedFrame["ip"]) + 0x9e3779b9 + (stackHash<<6) + (stackHash>>2)
            stackHash ^= hash(parsedFrame["obj"]) + 0x9e3779b9 + (stackHash<<6) + (stackHash>>2)
            localObj = localObj or not parsedFrame["obj"].startswith("/cvmfs/cms.cern.ch/")
            frames.append(parsedFrame)
    return {"frames":frames,"hash":stackHash,"local":localObj}
    
def printStack(frameList):
    for i,frame in enumerate(frameList):

        print " ",frame["fn"]
        if not frame["obj"].startswith("/cvmfs/cms.cern.ch/"):
            if frame.has_key("lno"):
                print "  |  ",frame["file"],"["+frame["lno"]+"]"
            print "  |  ",frame["obj"]
    
def parseLeakInfo(error):
    what = error.find("xwhat")
    if what is not None:
        leakedBytes = what.find("leakedbytes")
        leakedBlocks = what.find("leakedblocks")
        if leakedBytes is not None and leakedBlocks is not None:
            return {"bytes":int(leakedBytes.text), "blocks":int(leakedBlocks.text)}

def parseMemcheckReport(xmlFile):
    tree = ET.parse(xmlFile)
    root = tree.getroot()
    reportSummary = {}
    for child in root.iterfind("error"):
        kind = child.find("kind")
        stack = child.find("stack")
        if kind is not None and kind.text.startswith("Leak") and stack is not None:
            if not reportSummary.has_key(kind.text):
                reportSummary[kind.text]={}
            stackInfo = parseStack(stack)
            report = {"leak":parseLeakInfo(child),"stack":stackInfo["frames"],"local":stackInfo["local"]}
            if reportSummary[kind.text].has_key(stackInfo["hash"]):
                reportSummary[kind.text][stackInfo["hash"]]["leak"]["bytes"]+=report["leak"]["bytes"]
                reportSummary[kind.text][stackInfo["hash"]]["leak"]["blocks"]+=report["leak"]["blocks"]
            else:
                reportSummary[kind.text][stackInfo["hash"]]=report
    return reportSummary
   
                
reportSummary1 = parseMemcheckReport(args.reference[0])
reportSummary2 = parseMemcheckReport(args.reference[1])

for kind,reports in reportSummary2.iteritems():
    #if kind!="Leak_DefinitelyLost":
    #    continue
    for stackHash in reports.keys():
        if not reportSummary1[kind].has_key(stackHash):
            '''
            print kind,reportSummary2[kind][stackHash]["leak"]["bytes"]
            printStack(reportSummary2[kind][stackHash]["stack"])
            print
            '''
            pass
        elif reportSummary2[kind][stackHash]["leak"]["bytes"]>reportSummary1[kind][stackHash]["leak"]["bytes"]:
            print kind,reportSummary1[kind][stackHash]["leak"]["bytes"],"->",reportSummary2[kind][stackHash]["leak"]["bytes"]
            printStack(reportSummary2[kind][stackHash]["stack"])
            print
                 

