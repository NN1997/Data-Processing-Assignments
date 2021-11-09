#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cursor = openconnection.cursor()

    cursor.execute("select PartitionNum from rangeratingsmetadata where minrating between "+ str(ratingMinValue) +" and "+ str(ratingMaxValue) + " OR maxrating between "+ str(ratingMinValue) +" and "+ str(ratingMaxValue) +";")
    RangePartNum = cursor.fetchall()

    result = []
    for part in RangePartNum:
        partname = "RangeRatingsPart" + str(part[0])
        cursor.execute("select * from " + partname + " where rating between " + str(ratingMinValue) + " and " + str(ratingMaxValue) + ";" )
        rangeresult = cursor.fetchall()
        for i in rangeresult:
            result.append([partname] + list(i))



    cursor.execute("select table_name from information_schema.tables where table_name like 'roundrobinratingspart%';")
    RoundPartNum = cursor.fetchall()

    for part in range(len(RoundPartNum)):
        partname = "RoundRobinRatingsPart" + str(part)
        cursor.execute("select * from " + partname + " where rating between " + str(ratingMinValue) + " and " + str(ratingMaxValue) + ";")
        roundresult = cursor.fetchall()
        for i in roundresult:
            result.append([partname] + list(i))


    writeToFile("RangeQueryOut.txt", result)
    #return (len(result))



def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursor = openconnection.cursor()

    cursor.execute("select partitionnum from rangeratingsmetadata where " + str(ratingValue) + " between minrating and maxrating;")
    RangePartNum = cursor.fetchall()

    result = []
    for part in RangePartNum:
        partname = "RangeRatingsPart" + str(part[0])
        cursor.execute("select * from " + partname + " where rating=" + str(ratingValue) + ";")
        rangeresult = cursor.fetchall()
        for i in rangeresult:
            result.append([partname] + list(i))



    cursor.execute("select table_name from information_schema.tables where table_name like 'roundrobinratingspart%';")
    RoundPartNames = cursor.fetchall()

    for part in range(len(RoundPartNames)):
        partname = "RoundRobinRatingsPart" + str(part)
        cursor.execute("select * from " + partname + " where rating=" + str(ratingValue) +";")
        roundresult = cursor.fetchall()
        for i in roundresult:
            result.append([partname] + list(i))



    writeToFile("PointQueryOut.txt",result)
    #return (len(result))



def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
