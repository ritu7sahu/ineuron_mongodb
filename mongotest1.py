import pymongo
client = pymongo.MongoClient("mongodb+srv://shubhsahu:Shubh123@cluster0.mrxjb.mongodb.net/?retryWrites=true&w=majority")
db = client.test

data = {
    "name" : "Ritu",
    "mail_id" : "ritu@gmail.com",
    "subjects":["data science","big data","data analytics"]

}
database = client['mongotest']
collection = database['stud']
collection.insert_one(data)