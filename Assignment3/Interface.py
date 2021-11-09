#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + ratingstablename)
    #cur = conn.cursor()
    cursor.execute("CREATE TABLE "+ratingstablename+" (UserID INTEGER, random1 VARCHAR, MovieID INTEGER, random2 VARCHAR, Rating FLOAT, random3 VARCHAR, Timestamp INT)")

    with open(ratingsfilepath, 'r') as f:
        cursor.copy_from(f, ratingstablename, sep=':',columns=('UserID','random1','MovieID','random2','Rating','random3','Timestamp'))

    cursor.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN random1, DROP COLUMN random2,DROP COLUMN random3, DROP COLUMN Timestamp")
    cursor.close()
    openconnection.commit()
    pass


def rangePartition(ratingstablename, numberofpartitions, openconnection):

    #print(numberofpartitions)
    cursor = openconnection.cursor()
    ratingrange = 5/float(numberofpartitions)
    ratingrange1 = round(ratingrange,2)
    #print("Rating Range: ", ratingrange1)

    lower = 0
    upper = ratingrange1


    for i in range(0, numberofpartitions):
        Tname = 'range_part' + str(i)
        cursor.execute("DROP TABLE IF EXISTS " + Tname + ";")
        #print("Iteration ",i," DROPPED TABLE",Tname)
        if (i == 0):
            #print("Iteration ", i, " CREATING TABLE",Tname)
            cursor.execute("CREATE TABLE " + Tname + " AS  SELECT * FROM " + ratingstablename + " WHERE rating >= " + str(lower) + " AND rating <= " + str(upper) + ";")
            #cursor.execute("SELECT COUNT(*) FROM " + Tname + ";")
            #rows = cursor.fetchall()[0][0]
            #print(rows)
        else:
            #print("Iteration ", i, " CREATING TABLE",Tname)
            cursor.execute("CREATE TABLE " + Tname + " AS  SELECT * FROM " + ratingstablename + " WHERE rating > " + str(lower) + " AND rating <= " + str(upper) + ";")
            #cursor.execute("SELECT COUNT(*) FROM " + Tname + ";")
            #rows = cursor.fetchall()[0][0]
            #print(rows)
        #print("Lower: ", lower, " Upper ", upper)

        lower = upper
        upper = lower + ratingrange1

    openconnection.commit()
    cursor.close()

#rangePartition('ratings',4,getOpenConnection('postgres','1234','postgres'))

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()

    for i in range(numberofpartitions):
        Tname = 'roundrobin_part' + str(i)
        cursor.execute("DROP TABLE IF EXISTS " + Tname)
        cursor.execute("CREATE TABLE "+ Tname +" (UserID INTEGER, MovieID INTEGER, Rating FLOAT);")

    cursor.execute("SELECT * FROM " + ratingstablename + ";")
    rows=cursor.fetchall()

    count = 0
    for row in rows:
        Tname = 'roundrobin_part' + str(count)
        cursor.execute('INSERT INTO ' + Tname + ' VALUES(' + str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]) + ');')
        count = (count+1)%numberofpartitions

    openconnection.commit()
    cursor.close()



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()

    insertQuery = 'INSERT INTO ' + ratingstablename + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');'
    cursor.execute(insertQuery)

    cursor.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + "roundrobin_part" + "%';")
    numberofparitions = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM " + ratingstablename + ";")
    numberofrows = (cursor.fetchall())[0][0]
    parition = (numberofrows- 1) % numberofparitions

    table_name = 'roundrobin_part' + str(parition)
    cursor.execute('INSERT INTO ' + table_name + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');')

    openconnection.commit()
    cursor.close()



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    #print(ratingstablename)
    cur.execute('INSERT INTO ' + ratingstablename + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');')
    #cur.execute('SELECT * FROM range_part1 LIMIT 10;')
    #res = cur.fetchall()
    #print(res)
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + "range" + "%';")
    numberofpartitions = cur.fetchall()[0][0]
    #print(numberofpartitions)

    range = round((5 / numberofpartitions), 2)
    partition = int(rating / range)
    if (rating / range) != 0 and (rating % range) == 0:
        partition = partition - 1

    tname = 'range_part' + str(partition)
    cur.execute('INSERT INTO ' + tname + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');')

    openconnection.commit()
    cur.close()

#rangeinsert('ratings',12,123,4,getOpenConnection('postgres','1234','postgres'))

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
