```python
# İlk önce veri setimiz hdfs servisinin içerisinde mi onu kontrol ediyoruz.
! hdfs dfs -ls /user/train/datasets
Found 8 items
.
.
.
.
drwxr-xr-x - train supergroup          2021-01-02 12:05 /user/train/datasets/house-prices

! hdfs dfs -put /home/train/datasets/house-prices/ /user/train/datasets

import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F
import pandas as pd

spark = (
SparkSession.builder
		.appName("House Price Regression")
		.master("yarn")
		.config("spark.sql.shuffle.partitions", "2")
		.getOrCreate()

# Read Data #
df = (spark.read.format("csv")
			.option("header",True)
			.option("inferSchema",True)
			.load("/user/train/datasets/house-prices/train.csv"))

# Data Explore #
df.persist()
# Bütün columnların isimleri bir dataframe gibi gözükmektedir.

print(df.count())
1460

print(len(df.columns))
81

pd.set_option("display.max_columns",None)
pd.set_option("display.max_rows",None)

df.limit(5).toPandas()

df.printSchema()
root
	|-- Id: integer (nullable = true)
	|-- MSSubClass: integer (nullable = true)
	|-- MSZoning: string (nullable = true)
	|-- LotFrontage: string (nullable = true)
	|-- LotArea: integer (nullable = true)
	|-- Street: string (nullable = true)
	|-- Alley: string (nullable = true)
	|-- LotShape: string (nullable = true)
	|-- LandContour: string (nullable = true)
	|-- Utilities: string (nullable = true)
	|-- LotConfig: string (nullable = true)
	|-- LandSlope: string (nullable = true)
	|-- Neighborhood: string (nullable = true)
	|-- Condition1: string (nullable = true)
	|-- Condition2: string (nullable = true)
	|-- BldgType: string (nullable = true)
	|-- HouseStyle: string (nullable = true)
	|-- OverallQual: integer (nullable = true)
	|-- OverallCond: integer (nullable = true)
	|-- YearBuilt: integer (nullable = true)
	|-- YearRemondAdd: integer (nullable = true)
	|-- RoofStyle: string (nullable = true)
	|-- RoofMatl: string (nullable = true)
	|-- 

# ('LotFrontage', 'string') --> int
# ('MasVnrArea', 'string'), --> int
# ('GarageYrBlt', 'string'), --> int
# Bu 3 sütunun spark tarafından integer yerine string yapıldığını gördük ve bunları düzeltmemiz gerekmektedir.
df.select("LotFrontage").filter("LotFrontage == 'NA'").show()
df.select("MasVnrArea").filter("MasVnrArea == 'NA'").show()
df.select("GarageYrBlt").filter("GarageYrBlt == 'NA'").show()
# Bu 3 sütunun içerisinde NA değerler olduğu için yakaladığı string değerler yüzünden integer yerine string yapmış.

# Cast mis-inferred dtypes #
# NA değerleri doldurabilmek için ortalamasını alabiliriz.
df.selectExpr("AVG("LotFrontage")").show()
+--------------------------------+
|avg(CAST(LotFrontage AS DOUBLE))|
+--------------------------------+
|               70.04995836892665|
+--------------------------------+

df.selectExpr("AVG("MasVnrArea")").show()
+-------------------------------+
|avg(CAST(MasVnrArea AS DOUBLE))|
+-------------------------------+
|             103.68526170798899|
+-------------------------------+

df.selectExpr("AVG("GarageYrBlt")").show()
+--------------------------------+
|avg(CAST(GarageYrBlt AS DOUBLE))|
+--------------------------------+
|              1978.5061638868744|
+--------------------------------+

df1 = df.withColumn("LotFrontage", F.when(F.col("LotFrontage") == 'NA', 70).otherwise(F.col("LotFrontage"))) \
				.withColumn("LotFrontage", F.col("LotFrontage").cast("int")) \
				.withColumn("MasVnrArea", F.when(F.col("MasVnrArea") == 'NA', 104).otherwise(F.col("MasVnrArea"))) \
				.withColumn("MasVnrArea", F.col("MasVnrArea").cast("int")) \
				.withColumn("GarageYrBlt", F.when(F.col("GarageYrBlt") == 'NA', 1978).otherwise(F.col("GarageYrBlt"))) \
				.withColumn("GarageYrBlt", F.col("GarageYrBlt").cast("int")) \

df.select("LotFrontage", "MasVnrArea", "GarageYrBlt").printSchema()
root
	|-- LotFrontage: string (nullable = true)
	|-- MasVnrArea: string (nullable = true)
	|-- GarageYrBlt: string (nullable = true)

df1.select("LotFrontage", "MasVnrArea", "GarageYrBlt").printSchema()
root
	|-- LotFrontage: integer (nullable = true)
	|-- MasVnrArea: integer (nullable = true)
	|-- GarageYrBlt: integer (nullable = true)

# Null Check #
df1.select("FireplaceQu").filter(F.col("FireplaceQu").isnull() == "NA").show()
+-----------+
|FireplaceQu|
+-----------+
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
|         NA|
+-----------+

def null_count(df, col_name):
		nc = df1.select(col_name).filter(
				(F.col(col_name) == "NA") |)
				(F.col(col_name) == "") |)
				(F.col(col_name).isNull())).count()
		return nc

nnn_cc = null_count(df1, "FireplaceQu")
print(nnn_cc)
690
# 690 tane yazdığımız fonksiyona uygun koşul vardır.

for col_name in df1.dtypes:
		nc = null_count(df1, col_name[0])
		if nc > 0:
				print("{} has {}.".format(col_name[0],nc))
Alley has 1360.
MasVnrType has 8.
BsmtQual has 37.
BsmtCond has 37.
BsmtExposure has 38.
BsmtFinType1 has 37.
BsmtFinType2 has 38.
Electrical has 1.
FireplaceQu has 690.
GarageType has 81.
GarageFinish has 81.
GarageQual has 81.
GarageCond has 81.
PoolQC has 1453.
Fence has 1179.
MiscFeature has 1406.

# Yüzdelik değer olarak hesaplamak istersek;
df_count = df.count()
print(df_count)
1460

for col_name in df1.dtypes:
		nc = null_count(df1, col_name[0])
		if nc > 0:
				print("{} has {} % {}".format(col_name[0], nc, (nc/df_count)*100))
Alley has 1360 %93.76712328767123
MasVnrType has 8 % 0.547945205479452
BsmtQual has 37 % 2.5342465753424652
BsmtCond has 37 % 2.5342465753424652
BsmtExposure has 2.6027397260273974
BsmtFinType1 has 37 % 2.5342465753424652
BsmtFinType2 has 38 % 2.6027397260273974
Electrical has 1 0.0684931506849315
FireplaceQu has 690 % % 47.26027397260274
GarageType has 81 5.5479452054794525
GarageFinish has 81 % 5.5479452054794525
GarageQual has 81 % 5.5479452054794525
GarageCond has 81 % 5.5479452054794525
PoolQC has 1453 % 99.52054794520548
Fence has 1179 % 80.75342465753424
MiscFeature has 1406 % 96.30136986301369

# Group Columns
categoric_cols = []

numeric_cols = []

label_col = ['SalePrice']
# Modele katkısı olmayan sütunları ayırmak için discarted_cols adında bir liste oluşturuyoruz.
discarted_cols = ['Id','Alley','FireplaceQu','PoolQC', 'Fence', 'MiscFeature']
# Alley, FireplaceQu, PoolQC, Fence, MiscFeature have too much nulls.
# Id has too many categories.

for col_name in df1.dtypes:
		if (col_name[0] not in label_col+discarted_col):
				if col_name[1] == 'string':
						categoric_cols.append(col_name[0])
				else: 
						numeric_cols.append(col_name[0])

print(categoric_cols)
print(len(categoric_cols))
.
.
.
38
print(numeric_cols)
print(len(numeric_cols))
.
.
.
36
print(discarted_cols)
print(len(discarted_cols))
.
.
.
6
print(label_col)
print(len(label_cols))
.
.
.
1

# Column Check
if len(df1.columns) == (len(label_col) + len(discated_cols) + len(numeric_cols) + len(categoric_cols)):
		print("Column check is True")
else: print("There is problem for column check")
Column check is True

# Examine Categoricals
for cat_col in categoric_cols:
		print(cat_col)
		df1.groupBy(cat_col).count().orderBy(F.desc("count")).show()
MSZoning
+--------+-----+
|MSZoning|count|
+--------+-----+
|      RL| 1151|
|      RM|  218|
|      FV|   65|
|      RH|   16|
| C (all)|   10|
+--------+-----+

Street
+------+-----+
|Street|count|
+------+-----+
|  Pave| 1454|
|  Grvl|    6|
+------+-----+

LotShape
+--------+-----+
|LotShape|count|
+--------+-----+
|     Reg|  925|
|     IR1|  484|
|     IR2|   41|
|     IR3|   10|
+--------+-----+

LandContour
+-----------+-----+
|LandContour|count|
+-----------+-----+
|        Lvl| 1311|
|        Bnk|   63|
|        HLS|   50|
|        Low|   36|
+-----------+-----+

binary_cat_cols = ['Street','CentralAir']
# Utilities, Condition2, RoofMatl, Heating has one week cats and binary, should be discarted.
# Neighborhood look at distinct count.

discarted_cols = ['Id','Alley','FireplaceQu','PoolQC','Fence','MiscFeature','Utilities','Condition2','RoofMatl','Heating']
# discarted_cols listesini önceki koddaki yere eklersek bir çok feature değişecektir.

# StringIndexer
from pyspark.ml.feature import StringIndexer

my_dict = {}

string_indexer_objs = []

string_indexer_output_names = []

ohe_input_names = []

ohe_output_names = []

for col_name in categoric_cols:
		my_dict[col_name+"_index_obj"] = StringIndexer() \
		.setHandleInvalid("skip") \
		.setInputCol(col_name) \
		.setOutputCol(col_name+"_indexed")
		
		string_indexer_objs.append(my_dict.get(col_name+"_index_obj"))
		string_indexer_output_names.append(col_name+"_indexed")
		if col_name not in binary_cat_cols:
				ohe_input_names.append(col_name+"_indexed")
				ohe_output_names.append(col_name+"_ohe")
			 
print(string_indexer_objs)
print(len(string_indexer_objs))
34

print(string_indexer_output_names)
print(len(string_indexer_output_names))
34

print(ohe_input_names)
print(len(ohe_input_names))
32

print(ohe_output_names)
print(len(ohe_output_names))
32

# One-Hot-Encoder
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder() \
.setInputCols(ohe_input_names) \
.setOutputCols(ohe_output_names)

# VectorAssambler
not_to_hot_coded = list(set(string_indexer_output_names).difference(set(ohe_input_names)))
print(not_to_hot_coded)
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler() \
.setHandleInvalid("ski") \
.setInputCols(numeric_cols + not_to_hot_coded + ohe_output_names)
.setOutputCol("unscaled_features")

# Feature Scaling
from pyspark.ml.feature import StandardScaler
scaler = StandardScaler() \
.setInputCol("unscaled_features") \
.setOutputCol("features")

# Estimator
from pyspark.ml.regression import GBTRegressor

estimator = GBTRegressor(labelCol=label_col[0])

# Pipeline
from pyspark.ml import Pipeline
pipeline_obj = Pipeline().setStages(string_indexer_objs + [encoder, assembler, scaler, estimator])

# Split Dataset
train_df, test_df = df1.randomSplit([.8, .2], seed=142)
train_df.count()
1163

test_df.count()
297

# Train Model
pipeline_model = pipeline_obj.fit(train_df)

# Prediction
transformed_df = pipeline_model.transform(test_df)

transformed_df.select("SalePrice","prediction").show()
+---------+------------------+
|SalePrice|        prediction|
+---------+------------------+
|   140000|166418.00194302486|
|   129900| 186938.8536276622|
|   129500| 145038.8401122167|
|   157000|150866.06806781224|
|   154000| 156686.9315937667|
|   207500|189485.31450506533|
|   165500|158250.67025043225|
|   160000|  152051.549367772|
|   239686| 339009.4308244648|
|   127000|120698.47175147035|
|   110000| 94470.79060351106|
|   385000|  794776.825576351|
|   101000| 83558.52498552404|
|   245000| 231726.6914173506|
|   260000| 284801.2000961671|
|   204750|195432.63382324998|
|   214000|230876.43925416976|
|   198900|194057.01214842233|
|   169500| 177679.2237630042|
|   180000|150103.03444576386|
+---------+------------------+
only showing top 20 rows

# Evaluate Model
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(labelCol=label_col[0], metricName='r2')
evaluator.evaluate(transform_df)
0.7133529989253724
