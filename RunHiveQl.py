from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark import HiveContext
import sys
from decimal import *
from time import strftime,localtime
import numpy

getcontext().prec=18
decimalFormat=Decimal()
dateFormat=strftime('%Y-%m-%d',localtime())
timestampFormat=strftime('%Y-%m-%d%H:%M:%S.(%f/1000)')


def main():
   if len(sys.argv) > 2:
      scriptPath = sys.argv[1]
      resultPath = sys.argv[2]
   else:
      print "Missing Arguments"

   sc = SparkContext("local", "Test sql queries from pyspark")
   try:
	hsc=HiveContext(sc)
        scriptRaw=str(sc.textFile(scriptPath,use_unicode=False).cache().collect())
        print scriptRaw
        result=open(resultPath,'w')
        for i in scriptRaw.split(';'):
           i=i.replace('[\'','')
           i=i.replace('\']','')
           print i
	   if not i=="":
		df=hsc.sql(i.strip())
                df.show()
           def printSeparator(cols):
                print 'inside print' + str(cols)
                for j in range(0,cols):
                   print j
                   result.write("+----")
                result.write("+--+")             
           printHeader=True
           printFooter=False
           cols=df.columns
           print cols
           for row in df.collect():
                print str(row)
                if printHeader:
                   print str(len(cols))
		   printSeparator(len(cols))
                   for col in cols:
                      result.write("| " + col)  
                   result.write("|")
                   printSeparator(len(cols))
                   printHeader=False
                   printFooter=True
                for v in row:
                   print str(v)
                   result.write("|" + valueToString(v))
                result.write("|")    
           if(printFooter):
                printSeparator(len(cols))          
   except:
	sc.stop()
   
def valueToString(x):
   print str(x) 
   if isinstance(x, basestring):
        return str(x)
   if isinstance(x,Decimal):
        return decimalFormat.format(d)
   if isinstance(x,bytearray):
        return unicode(x,"utf-8"),
   if isinstance(x,seq):
        return ",".join(s.map(valueToString))
   if isinstance(x,datetime):
        return timestamp_func(x)
   if isinstance(x,Row):
	return row_func(x)
   if isinstance(x,null):
	return "NULL"
   print 'outside if'	
	 
def timestamp_func(t):
   str = timestampFormat.format(t)
   while(str.endswith("0")):
      str=str[0:len(str)-1]
   if str.endswith("."):
      str="0".join(str)
   str

def row_func(r):
   str=r.toSeq.zip(r.schema.fields).map(lambda (v,f): "\"%s\":%s".format(",".join(f.name,nestedValueToString(v))))
   str

def nestedValueToString(x):
   switcher = {
        s: lambda string: "\"%s\"".format(s),
        v: valueToString(v)
   }


if __name__ == '__main__':
   main()
      
	

