
# Exercise 4: Optimizing Redshift Table Design


```python
%load_ext sql
```


```python
from time import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd
```


```python
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")

```

# STEP 1: Get the params of the created redshift cluster 
- We need:
    - The redshift cluster <font color='red'>endpoint</font>
    - The <font color='red'>IAM role ARN</font> that give access to Redshift to read from S3


```python
# FILL IN THE REDSHIFT ENDPOINT HERE
# e.g. DWH_ENDPOINT="redshift-cluster-1.csmamz5zxmle.us-west-2.redshift.amazonaws.com" 
DWH_ENDPOINT="dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws.com"
    
#FILL IN THE IAM ROLE ARN you got in step 2.2 of the previous exercise
#e.g DWH_ROLE_ARN="arn:aws:iam::988332130976:role/dwhRole"
DWH_ROLE_ARN="arn:aws:iam::988332130976:role/dwhRole"
```

# STEP 2: Connect to the Redshift Cluster


```python
import os 
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string
```

# STEP 3: Create Tables
- We are going to use a benchmarking data set common for benchmarking star schemas in data warehouses.
- The data is pre-loaded in a public bucket on the `us-west-2` region
- Our examples will be based on the Amazon Redshfit tutorial but in a scripted environment in our workspace.

![afa](images/table_design.png)

## 3.1 Create tables (no distribution strategy) in the `nodist` schema


```python
%%sql 
CREATE SCHEMA IF NOT EXISTS nodist;
SET search_path TO nodist;

DROP TABLE IF EXISTS part cascade;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS dwdate;
DROP TABLE IF EXISTS lineorder;

CREATE TABLE part 
(
  p_partkey     INTEGER NOT NULL,
  p_name        VARCHAR(22) NOT NULL,
  p_mfgr        VARCHAR(6) NOT NULL,
  p_category    VARCHAR(7) NOT NULL,
  p_brand1      VARCHAR(9) NOT NULL,
  p_color       VARCHAR(11) NOT NULL,
  p_type        VARCHAR(25) NOT NULL,
  p_size        INTEGER NOT NULL,
  p_container   VARCHAR(10) NOT NULL
);

CREATE TABLE supplier 
(
  s_suppkey   INTEGER NOT NULL,
  s_name      VARCHAR(25) NOT NULL,
  s_address   VARCHAR(25) NOT NULL,
  s_city      VARCHAR(10) NOT NULL,
  s_nation    VARCHAR(15) NOT NULL,
  s_region    VARCHAR(12) NOT NULL,
  s_phone     VARCHAR(15) NOT NULL
);

CREATE TABLE customer 
(
  c_custkey      INTEGER NOT NULL,
  c_name         VARCHAR(25) NOT NULL,
  c_address      VARCHAR(25) NOT NULL,
  c_city         VARCHAR(10) NOT NULL,
  c_nation       VARCHAR(15) NOT NULL,
  c_region       VARCHAR(12) NOT NULL,
  c_phone        VARCHAR(15) NOT NULL,
  c_mktsegment   VARCHAR(10) NOT NULL
);

CREATE TABLE dwdate 
(
  d_datekey            INTEGER NOT NULL,
  d_date               VARCHAR(19) NOT NULL,
  d_dayofweek          VARCHAR(10) NOT NULL,
  d_month              VARCHAR(10) NOT NULL,
  d_year               INTEGER NOT NULL,
  d_yearmonthnum       INTEGER NOT NULL,
  d_yearmonth          VARCHAR(8) NOT NULL,
  d_daynuminweek       INTEGER NOT NULL,
  d_daynuminmonth      INTEGER NOT NULL,
  d_daynuminyear       INTEGER NOT NULL,
  d_monthnuminyear     INTEGER NOT NULL,
  d_weeknuminyear      INTEGER NOT NULL,
  d_sellingseason      VARCHAR(13) NOT NULL,
  d_lastdayinweekfl    VARCHAR(1) NOT NULL,
  d_lastdayinmonthfl   VARCHAR(1) NOT NULL,
  d_holidayfl          VARCHAR(1) NOT NULL,
  d_weekdayfl          VARCHAR(1) NOT NULL
);
CREATE TABLE lineorder 
(
  lo_orderkey          INTEGER NOT NULL,
  lo_linenumber        INTEGER NOT NULL,
  lo_custkey           INTEGER NOT NULL,
  lo_partkey           INTEGER NOT NULL,
  lo_suppkey           INTEGER NOT NULL,
  lo_orderdate         INTEGER NOT NULL,
  lo_orderpriority     VARCHAR(15) NOT NULL,
  lo_shippriority      VARCHAR(1) NOT NULL,
  lo_quantity          INTEGER NOT NULL,
  lo_extendedprice     INTEGER NOT NULL,
  lo_ordertotalprice   INTEGER NOT NULL,
  lo_discount          INTEGER NOT NULL,
  lo_revenue           INTEGER NOT NULL,
  lo_supplycost        INTEGER NOT NULL,
  lo_tax               INTEGER NOT NULL,
  lo_commitdate        INTEGER NOT NULL,
  lo_shipmode          VARCHAR(10) NOT NULL
);
```

## 3.1 Create tables (with a distribution strategy) in the `dist` schema


```python
%%sql

CREATE SCHEMA IF NOT EXISTS dist;
SET search_path TO dist;

DROP TABLE IF EXISTS part cascade;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS dwdate;
DROP TABLE IF EXISTS lineorder;

CREATE TABLE part (
  p_partkey     	integer     	not null	sortkey distkey,
  p_name        	varchar(22) 	not null,
  p_mfgr        	varchar(6)      not null,
  p_category    	varchar(7)      not null,
  p_brand1      	varchar(9)      not null,
  p_color       	varchar(11) 	not null,
  p_type        	varchar(25) 	not null,
  p_size        	integer     	not null,
  p_container   	varchar(10)     not null
);

CREATE TABLE supplier (
  s_suppkey     	integer        not null sortkey,
  s_name        	varchar(25)    not null,
  s_address     	varchar(25)    not null,
  s_city        	varchar(10)    not null,
  s_nation      	varchar(15)    not null,
  s_region      	varchar(12)    not null,
  s_phone       	varchar(15)    not null)
diststyle all;

CREATE TABLE customer (
  c_custkey     	integer        not null sortkey,
  c_name        	varchar(25)    not null,
  c_address     	varchar(25)    not null,
  c_city        	varchar(10)    not null,
  c_nation      	varchar(15)    not null,
  c_region      	varchar(12)    not null,
  c_phone       	varchar(15)    not null,
  c_mktsegment      varchar(10)    not null)
diststyle all;

CREATE TABLE dwdate (
  d_datekey            integer       not null sortkey,
  d_date               varchar(19)   not null,
  d_dayofweek	      varchar(10)   not null,
  d_month      	    varchar(10)   not null,
  d_year               integer       not null,
  d_yearmonthnum       integer  	 not null,
  d_yearmonth          varchar(8)	not null,
  d_daynuminweek       integer       not null,
  d_daynuminmonth      integer       not null,
  d_daynuminyear       integer       not null,
  d_monthnuminyear     integer       not null,
  d_weeknuminyear      integer       not null,
  d_sellingseason      varchar(13)    not null,
  d_lastdayinweekfl    varchar(1)    not null,
  d_lastdayinmonthfl   varchar(1)    not null,
  d_holidayfl          varchar(1)    not null,
  d_weekdayfl          varchar(1)    not null)
diststyle all;

CREATE TABLE lineorder (
  lo_orderkey      	    integer     	not null,
  lo_linenumber        	integer     	not null,
  lo_custkey           	integer     	not null,
  lo_partkey           	integer     	not null distkey,
  lo_suppkey           	integer     	not null,
  lo_orderdate         	integer     	not null sortkey,
  lo_orderpriority     	varchar(15)     not null,
  lo_shippriority      	varchar(1)      not null,
  lo_quantity          	integer     	not null,
  lo_extendedprice     	integer     	not null,
  lo_ordertotalprice   	integer     	not null,
  lo_discount          	integer     	not null,
  lo_revenue           	integer     	not null,
  lo_supplycost        	integer     	not null,
  lo_tax               	integer     	not null,
  lo_commitdate         integer         not null,
  lo_shipmode          	varchar(10)     not null
);
```

# STEP 4: Copying tables 

Our intent here is to run 5 COPY operations for the 5 tables respectively as show below.

However, we want to do accomplish the following:
- Make sure that the `DWH_ROLE_ARN` is substituted with the correct value in each query
- Perform the data loading twice once for each schema (dist and nodist)
- Collect timing statistics to compare the insertion times
Thus, we have scripted the insertion as found below in the function `loadTables` which
returns a pandas dataframe containing timing statistics for the copy operations

```sql
copy customer from 's3://awssampledbuswest2/ssbgz/customer' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';

copy dwdate from 's3://awssampledbuswest2/ssbgz/dwdate' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';

copy lineorder from 's3://awssampledbuswest2/ssbgz/lineorder' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';

copy part from 's3://awssampledbuswest2/ssbgz/part' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';

copy supplier from 's3://awssampledbuswest2/ssbgz/supplier' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';
```


## 4.1 Automate  the copying


```python
def loadTables(schema, tables):
    loadTimes = []
    SQL_SET_SCEMA = "SET search_path TO {};".format(schema)
    %sql $SQL_SET_SCEMA
    
    for table in tables:
        SQL_COPY = """
copy {} from 's3://awssampledbuswest2/ssbgz/{}' 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
        """.format(table,table, DWH_ROLE_ARN)

        print("======= LOADING TABLE: ** {} ** IN SCHEMA ==> {} =======".format(table, schema))
        print(SQL_COPY)

        t0 = time()
        %sql $SQL_COPY
        loadTime = time()-t0
        loadTimes.append(loadTime)

        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    return pd.DataFrame({"table":tables, "loadtime_"+schema:loadTimes}).set_index('table')
```


```python
#-- List of the tables to be loaded
tables = ["customer","dwdate","supplier", "part", "lineorder"]

#-- Insertion twice for each schema (WARNING!! EACH CAN TAKE MORE THAN 10 MINUTES!!!)
nodistStats = loadTables("nodist", tables)
distStats = loadTables("dist", tables)
```

## 4.1 Compare the load performance results


```python
#-- Plotting of the timing results
stats = distStats.join(nodistStats)
stats.plot.bar()
plt.show()
```

# STEP 5: Compare Query Performance


```python
oneDim_SQL ="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select sum(lo_extendedprice*lo_discount) as revenue
from lineorder, dwdate
where lo_orderdate = d_datekey
and d_year = 1997 
and lo_discount between 1 and 3 
and lo_quantity < 24;
"""

twoDim_SQL="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select sum(lo_revenue), d_year, p_brand1
from lineorder, dwdate, part, supplier
where lo_orderdate = d_datekey
and lo_partkey = p_partkey
and lo_suppkey = s_suppkey
and p_category = 'MFGR#12'
and s_region = 'AMERICA'
group by d_year, p_brand1
"""

drill_SQL = """
set enable_result_cache_for_session to off;
SET search_path TO {};

select c_city, s_city, d_year, sum(lo_revenue) as revenue 
from customer, lineorder, supplier, dwdate
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and (c_city='UNITED KI1' or
c_city='UNITED KI5')
and (s_city='UNITED KI1' or
s_city='UNITED KI5')
and d_yearmonth = 'Dec1997'
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
"""


oneDimSameDist_SQL ="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select lo_orderdate, sum(lo_extendedprice*lo_discount) as revenue  
from lineorder, part
where lo_partkey  = p_partkey
group by lo_orderdate
order by lo_orderdate
"""

def compareQueryTimes(schema):
    queryTimes  =[] 
    for i,query in enumerate([oneDim_SQL, twoDim_SQL, drill_SQL, oneDimSameDist_SQL]):
        t0 = time()
        q = query.format(schema)
        %sql $q
        queryTime = time()-t0
        queryTimes.append(queryTime)
    return pd.DataFrame({"query":["oneDim","twoDim", "drill", "oneDimSameDist"], "queryTime_"+schema:queryTimes}).set_index('query')
```


```python
noDistQueryTimes = compareQueryTimes("nodist")
distQueryTimes   = compareQueryTimes("dist") 
```


```python
queryTimeDF =noDistQueryTimes.join(distQueryTimes)
queryTimeDF.plot.bar()
plt.show()
```


```python
improvementDF = queryTimeDF["distImprovement"] =100.0*(queryTimeDF['queryTime_nodist']-queryTimeDF['queryTime_dist'])/queryTimeDF['queryTime_nodist']
improvementDF.plot.bar(title="% dist Improvement by query")
plt.show()
```
