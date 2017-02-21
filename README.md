# Global Watch

Insight Data Engineering</br>
Silicon Valley Session 2017A</br>

##Introduction
Global Watch is a tool designed to quickly give users insight to past global trends for every country in the world. I am using data
from The GDELT Project, which uses a massive inventory of the world's news media to perform real-time monitoring of every
accessible print, broadcast, and online news report around the globe. Through the tool I created anyone can travel back in time till
1979 and find and understand news patterns for different countries in the world.

##Motivation
My family is originally from Ecuador and I grew up watching a Ecuavisa, one of the major television networks in Ecuador, Univision,
a broadcast telivision network that targets Latino Americans, and CNN, an American television channel. Throughout my life I have noticed the difference that exists in the kinds of news
people get based on the country they live in. Due to the limited variety of news we receive at home, we unconsciously
place ourselves in a bubble and are only aware of what is relevant to our home country. My main goal for the Insight Data
Engineering Project was to create a tool that would allows users to quickly zoom out of the bubble and understand from
a global perspective what is important to other countries in the world.

##What does my project do?
###Supported Queries
A user can query news for a specific day, month, or year for any given country dating back to 1979.

###Data Source
Over 100GB of news data from Events 1.0, a dataset that is part of The GDELT Project. (https://aws.amazon.com/public-datasets/gdelt/)

##Pipeline
![Alt text](images/pipelinepicture.png)

##Database Design
###Why Casssandra?
Textbook Definition:</br>
Apache Cassandra is an open source, distributed, decentralized, elastically scalable,
highly available, fault-tolerant, tuneably consistent, row-oriented database that bases
its distribution design on Amazon’s Dynamo and its data model on Google’s Bigtable.

For my use case:
Distributed: 
 *I set up a distributed environment and Cassandra is capable of runnning on multiple machines.
Decentralized:

The problem I solved worked perfectly with Cassandra. I needed to use a database that satified two categories from the
CAP Theorem - availability and partition tolerance. In order to display country news data to users quickly and reliably I needed a database that efficiently stored all of the timeseries data I was dealing with. </br>


##Results
