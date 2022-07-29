import logging
logging.basicConfig(filename='sql.log',level=logging.DEBUG,format="%(levelname)s, %(asctime)s, %(message)s")
logging.info("importing mysql connector, pandas,pymongo")
import mysql.connector as connection
import pandas as pd
import pymongo
logging.info("connecting mongodb and mysql")
client = pymongo.MongoClient("mongodb+srv://shubhsahu:Shubh123@cluster0.mrxjb.mongodb.net/?retryWrites=true&w=majority")
db = client.test
mydb = connection.connect(host="localhost",user="root",password="Root@123")
cursor = mydb.cursor()
#cursor.execute("create database sqlAssignments")
#cursor.execute("show databases")
#print(cursor.fetchall())

class sql_assignments:
    """ Create a  table attribute dataset and dress dataset """
    def creatingTable(self):
        try:
            logging.info("creating tables attribute_dataset & dress_sales")
            attr_table = 'create table if not exists sqlAssignments.attribute_dataset( Dress_ID int(20),Style varchar(20),Price varchar(20),Rating decimal(2,1),Size varchar(20),season varchar(20),NeckLine varchar(20),SleeveLength varchar(20),waiseline varchar(20),Material varchar(20),FabricType varchar(20),Decoration varchar(20),PatternType varchar(20),Recommendation int(10))'
            cursor.execute(attr_table)

            dress_table = 'create table if not exists sqlAssignments.dress_sales(Dress_ID int(20),`29/8/2013` int(20),`31/8/2013` int(20),`2/9/2013` int(20),`4/9/2013` int(20),`6/9/2013` int(20),`8/9/2013` int(20),`10/9/2013` int(20),`12/9/2013` int(20),`14/9/2013` int(20),`16/9/2013` int(20),`18/9/2013` int(20),`20/9/2013` int(20),`22/9/2013` int(20),`24/9/2013` int(20),`26/9/2013` int(20),`28/9/2013` int(20),`30/9/2013` int(20),`2/10/2013` int(20),`4/10/2013` int(20),`6/10/2013` int(20),`8/10/2010` int(20),`10/10/2013` int(20),`12/10/2013` int(20))'
            cursor.execute(dress_table)
        except Exception as e:
            logging.exception(e)

    """ Do a bulk load for these two table for respective dataset """
    def doBulkLoadWithFor(self):
        logging.info("Do a bulk load for these two table for respective dataset")
        logging.info("inserting Attribute DataSet data in bulk")
        try:
            logging.info("Reading excel Attribute DataSet file")
            df = pd.read_excel(r'C:\Users\DELL\Desktop\Python learning\Attribute DataSet.xlsx')
            logging.info("Iterating dataframe & inserting records")
            for (rows, rs) in df.iterrows():
                qry = "insert into sqlassignments.attribute_dataset values(""'" + str(rs[0]) + "','" + str(
                    rs[1]) + "','" + str(rs[2]) + "','" + str(rs[3]) + "','" + str(rs[4]) + "','" + str(
                    rs[5]) + "','" + str(rs[6]) + "','" + str(rs[7]) + "','" + str(rs[8]) + "','" + str(
                    rs[9]) + "','" + str(rs[10]) + "','" + str(rs[11]) + "','" + str(
                    rs[12]) + "','" + str(rs[13]) + "')"
                cursor.execute(qry)
        except Exception as e:
            logging.exception(e)

        logging.info("inserting dress sales dataset in bulk")
        try:
            logging.info("Reading excel dress sales dataset file")
            df = pd.read_excel(r'C:\Users\DELL\Desktop\Python learning\Dress Sales.xlsx')
            logging.info("Iterating dataframe & inserting records for dress sales")
            for (rows, rs) in df.iterrows():
                qry = "insert into sqlassignments.dress_sales values(""'" + str(rs[0]) + "','" + str(
                    rs[1]) + "','" + str(rs[2]) + "','" + str(rs[3]) + "','" + str(rs[4]) + "','" + str(
                    rs[5]) + "','" + str(rs[6]) + "','" + str(rs[7]) + "','" + str(rs[8]) + "','" + str(
                    rs[9]) + "','" + str(rs[10]) + "','" + str(rs[11]) + "','" + str(rs[12]) + "','" + str(
                    rs[13]) + "','" + str(rs[14]) + "','" + str(rs[15]) + "','" + str(rs[16]) + "','" + str(
                    rs[17]) + "','" + str(rs[18]) + "','" + str(rs[19]) + "','" + str(rs[20]) + "','" + str(
                    rs[21]) + "','" + str(rs[22]) + "','" + str(rs[23]) + "')"

                cursor.execute(qry)
        except Exception as e:
            logging.exception(e)

    """ Do a bulk load for these attribute table using load Data Local In file """
    def loadDataLocalInfile(self):
        logging.info("Do a bulk load for these two table for respective dataset using load data local in file")
        logging.info("inserting Attribute DataSet data in bulk")
        try:
            logging.info("creating new table attribute_dataset1")
            attr_table = 'create table if not exists sqlAssignments.attribute_dataset1( Dress_ID int(20),Style varchar(20),Price varchar(20),Rating decimal(2,1),Size varchar(20),season varchar(20),NeckLine varchar(20),SleeveLength varchar(20),waiseline varchar(20),Material varchar(20),FabricType varchar(20),Decoration varchar(20),PatternType varchar(20),Recommendation int(10))'
            cursor.execute(attr_table)
            logging.info("Reading excel Attribute DataSet file")
            df = pd.read_excel(r'C:\Users\DELL\Desktop\Python learning\Attribute DataSet.xlsx')
            logging.info("Inserting records using load data local infile")
            query = "load data local infile 'C:/Users/DELL/Downloads/employee.csv' into table sqlAssignments.attribute_dataset1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"'  ESCAPED BY '\\\\' lines terminated by '\n' ignore 1 rows"
            cursor.execute(query)
        except Exception as e:
            logging.exception(e)

    """ read these dataset in pandas as a dataframe """
    def readDatasetAsDataframe(self):
        logging.info("inside read  dataset in pandas as a dataframe")
        try:
            logging.info("getting data from attribute_dataset")
            cursor.execute('select * from sqlassignments.attribute_dataset')
            rows_attribute = cursor.fetchall()
            logging.info("getting attribute_dataset dataframe using pandas")
            dataframe_attribute = pd.DataFrame(rows_attribute, index=None,columns=['Dress_ID', 'Style', 'Price', 'Rating', 'Size', 'Season','NeckLine', 'SleeveLength', 'waiseline', 'Material', 'FabricType', 'Decoration', 'Pattern Type','Recommendation'])
            logging.info("attribute_dataset "+dataframe_attribute)

            logging.info("getting data from dress_sales")
            cursor.execute('select * from sqlassignments.dress_sales')
            rows_dress = cursor.fetchall()
            logging.info("getting rows_dress dataframe using pandas")
            dataframe_sales = pd.DataFrame(rows_dress, index=None,columns=['Dress_ID','29/8/2013' ,'31/8/2013' ,'2/9/2013' ,'4/9/2013' ,'6/9/2013' ,'8/9/2013' ,'10/9/2013' ,'12/9/2013' ,'14/9/2013' ,'16/9/2013' ,'18/9/2013' ,'20/9/2013' ,'22/9/2013' ,'24/9/2013' ,'26/9/2013' ,'28/9/2013' ,'30/9/2013' ,'2/10/2013' ,'4/10/2013' ,'6/10/2013' ,'8/10/2010' ,'10/10/2013' ,'12/10/2013'])
            logging.info("dress_sales_dataset " + dataframe_sales)
        except Exception as e:
            logging.exception(e)

    """ Convert attribute dataset in json format  """
    def convertDatasetToJson(self):
        logging.info("inside Covert dataset to json function")
        try:
            cursor.execute('select * from sqlassignments.attribute_dataset')
            rows_attribute = cursor.fetchall()
            logging.info("getting attribute_dataset dataframe using pandas")
            dataframe_attribute = pd.DataFrame(rows_attribute, index=None,columns=['Dress_ID', 'Style', 'Price', 'Rating', 'Size', 'Season','NeckLine', 'SleeveLength', 'waiseline', 'Material','FabricType', 'Decoration', 'Pattern Type', 'Recommendation'])
            json = dataframe_attribute.to_json(orient='records')
            logging.info("converted dataframe to json "+json)
        except Exception as e:
            logging.exception(e)

    """  Store this dataset into mongodb """
    def storeDatatoMongo(self):
        logging.info("inside storDatatoMongo function")
        try:
            cursor.execute('select * from sqlassignments.attribute_dataset')
            rows_attribute = cursor.fetchall()
            logging.info("getting attribute_dataset dataframe using pandas")
            dataframe_attribute = pd.DataFrame(rows_attribute, index=None,
                                               columns=['Dress_ID', 'Style', 'Price', 'Rating', 'Size', 'Season',
                                                        'NeckLine', 'SleeveLength', 'waiseline', 'Material',
                                                        'FabricType', 'Decoration', 'Pattern Type', 'Recommendation'])
            logging.info("converted data to json")
            json = dataframe_attribute.to_json(orient='records')
            logging.info("created database dataset")
            database = client['dataset']
            logging.info("created collection attribute_dataset")
            collection = database['attribute_dataset']
            logging.info("inserted json data")
            collection.insert_many(json)
        except Exception as e:
            logging.exception(e)

    """in sql task try to perform left join operation with attrubute dataset and dress dataset on column Dress_ID"""
    def performLeftJoin(self):
        logging.info("Inside left join operation function")
        try:
            logging.info("query to join")
            cursor.execute("select * from sqlassignments.attribute_dataset left join sqlassignments.dress_sales on sqlassignments.attribute_dataset.Dress_ID = sqlassignments.dress_sales.Dress_ID")
            s = cursor.fetchall()
            logging.info('records after left join '+str(s))
        except Exception as e:
            logging.exception(e)

    """ Write a sql query to find out how many unique dress that we have based on dress id """
    def findUniqueDress(self):
        logging.info("inside findUniqueDress function")
        try:
            logging.info("query to find distinct records")
            cursor.execute("select DISTINCT(Dress_ID) from dress_sales")
            s = cursor.fetchall()
            logging.info("Distinct records are "+str(s))
        except Exception as e:
            logging.exception(e)

    """Try to find out how mnay dress is having recommendation 0"""
    def havingZeroRecommendation(self):
        logging.info("inside havingZeroRecommendation function")
        try:
            logging.info("query to dresses is having recommendation 0")
            cursor.execute("select count(Dress_ID) from attribute_dataset where Recommendation = 0")
            s = cursor.fetchall()
            logging.info("Records are "+str(s))
        except Exception as e:
            logging.exception(e)

    """Try to find out total dress sell for individual dress id """
    def totalDressSellForIndividualDressId(self):
        logging.info("inside totalDressSellForIndividualDressId function")
        try:
            logging.info("query to find out total dress sell for individual dress id")
            cursor.execute("select Dress_ID,(`29/8/2013` +`31/8/2013` +`2/9/2013` +`4/9/2013` +`6/9/2013` +`8/9/2013` +`10/9/2013` +`12/9/2013` +`14/9/2013` +`16/9/2013` +`18/9/2013` +`20/9/2013` +`22/9/2013` +`24/9/2013` +`26/9/2013` +`28/9/2013` +`30/9/2013` +`2/10/2013` +`4/10/2013` +`6/10/2013` +`8/10/2010` +`10/10/2013` +`12/10/2013`) as total from sqlassignments.dress_sales group by Dress_ID")
            s = cursor.fetchall()
            logging.info("Records are "+str(s))
        except Exception as e:
            logging.exception(e)

    """Try to find out a third highest most selling dress id """
    def thirdMostSellingDressId(self):
        logging.info("inside thirdMostSellingDressId function")
        try:
            logging.info("query to find out third most selling dress id")
            cursor.execute("select DISTINCT((`29/8/2013` +`31/8/2013` +`2/9/2013` +`4/9/2013` +`6/9/2013` +`8/9/2013` +`10/9/2013` +`12/9/2013` +`14/9/2013` +`16/9/2013` +`18/9/2013` +`20/9/2013` +`22/9/2013` +`24/9/2013` +`26/9/2013` +`28/9/2013` +`30/9/2013` +`2/10/2013` +`4/10/2013` +`6/10/2013` +`8/10/2010` +`10/10/2013` +`12/10/2013`)) as total,Dress_ID from sqlassignments.dress_sales order by total desc limit 1 OFFSET 2")
            s = cursor.fetchall()
            logging.info("dress ID with count "+str(s))
        except Exception as e:
            logging.exception(e)

logging.info("Creating Object of the class sql_assignments")
sqlObj = sql_assignments()
sqlObj.creatingTable()
sqlObj.doBulkLoadWithFor()
sqlObj.loadDataLocalInfile()
sqlObj.readDatasetAsDataframe()
sqlObj.convertDatasetToJson()
sqlObj.performLeftJoin()
sqlObj.findUniqueDress()
sqlObj.havingZeroRecommendation()
sqlObj.totalDressSellForIndividualDressId()
sqlObj.thirdMostSellingDressId()




