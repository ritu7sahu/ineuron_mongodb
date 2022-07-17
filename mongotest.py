import pymongo
client = pymongo.MongoClient("mongodb+srv://shubhsahu:Shubh123@cluster0.mrxjb.mongodb.net/?retryWrites=true&w=majority")
db = client.test
print(db)


d = {
    "name":"Ritu",
    "email" : "ritu@ineuron.ai",
    "surname" : "Sahu"
}
db1 = client['mongotest']
coll = db1['test']
coll.insert_one(d )
