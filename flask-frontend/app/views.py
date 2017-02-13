#jsonify creates a json representation of the response
from flask import request
from flask import jsonify
from flask import render_template
from app import app
from cassandra.cluster import Cluster
from collections import OrderedDict
import simplejson as json

# importing Cassandra modules from the driver we just installed
cluster = Cluster(['ec2-35-160-85-3.us-west-2.compute.amazonaws.com',
                    'ec2-52-38-0-155.us-west-2.compute.amazonaws.com',
                    'ec2-35-167-106-182.us-west-2.compute.amazonaws.com'])
session = cluster.connect('gdelt')

@app.route('/index')
def index():
  user = { 'nickname': 'Miguel' } # fake user
  mylist = [1,2,3,4]
  return render_template("index.html", title = 'Home', user = user, mylist = mylist)

#@app.route('/email')
#def email():
# return render_template("base.html")

@app.route("/cassandra_test")
def cassandra_test():
    cql = "SELECT * FROM testing"
    r = session.execute(cql)
    r_list = []
    for val in r:
        r_list.append(val)
    #jsonresponse = [{"Country":x[0],"Date":x[1],"Overview":x[2]} for x in r_list]
    jsonresponse = [{x[0],x[1],x[2]} for x in r_list]
    return str(jsonresponse)

@app.route('/overview')
def email():
 return render_template("email.html")

@app.route("/overview",methods=['POST'])
def email_post():
    emailid = request.json["country"]
    date = request.json["eventdate"]
    timeframe = request.json["timeframe"].lower()
    print(timeframe)
    cql = ""
    if timeframe == "daily":
	cql += "SELECT * FROM daily WHERE country = %s and date = %s"
    elif timeframe == "monthly":
	cql += "SELECT * FROM monthly WHERE country = %s and date = %s"
    elif timeframe == "yearly":
	cql += "SELECT * FROM yearly WHERE country = %s and date = %s"
    r = session.execute(cql,parameters = [emailid,int(date)])[0]
    def create_dict(ordermap):
	n_dict = {}
	for event in ordermap:
 	   n_dict[str(event)] = {str(key): value for key, value in ordermap[event].items()}
        return n_dict
    clean_dict = create_dict(r[2])
    total_mentions = clean_dict["TotalArticles"]['total']
    clean_dict.pop('TotalArticles',None)
    jsonresponse = {"Country":str(emailid),"Date":str(date),"Timeframe":str(timeframe),'Total':total_mentions,"Overview":clean_dict}
    print(jsonresponse)
    return jsonify(result=jsonresponse)
