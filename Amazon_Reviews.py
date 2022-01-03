#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import urllib.request
from PIL import Image
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd



spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
spark.sql('set spark.sql.caseSensitive=true')
df = spark.read.json("Electronics_new.json")
df = df.withColumn(
            'reviewTime',
                F.to_date(
                    F.unix_timestamp('reviewTime', 'MM d, yyyy').cast('timestamp')))



# In[2]:


#ASIN NUMARASINDAN URL ELDE EDER
def getProductURL(asin):
    return "https://www.amazon.com/dp/" + asin


# In[3]:


#Duplicate Review Sayısını bulur
df.groupBy(df.columns).count().where(F.col('count') > 1).select(F.sum('count')).show() #Task number 3


# In[4]:


#TUM ZAMANLARIN EN COK REVIEWA SAHIP URUNUNU BULUR
MostReviewedDF = df.groupBy("asin").count().orderBy('count', ascending=False) # task number 4
MostReviewedDF = MostReviewedDF.withColumn("count", MostReviewedDF["count"].cast(IntegerType()))

listOfMostRevievewProducts = MostReviewedDF.take(10)
for i in range(10):
    print(str(i+1) + ": " + getProductURL(listOfMostRevievewProducts[i].asin) + " " + str(listOfMostRevievewProducts[i][1])+ " reviews.")
   


# In[5]:


#HER YILA GÖRE EN KÖTÜ PUANA SAHİP ÜRÜNLERİN LİSTESİ VE GRAFİĞİ

adjustedDf = df.withColumn("reviewTime", to_date(col("reviewTime"), "yyyy-MM-dd"))    .withColumn('year', year("reviewTime"))

worst_products_per_year = adjustedDf.groupBy("year","asin").agg(avg("overall"),count("*"))    .where("count(1) > 20").groupBy("year").agg(first("asin"),min("avg(overall)"))


worst_products_per_year = worst_products_per_year.toPandas()
worst_products_per_year = worst_products_per_year.rename(columns={'year':'Year','first(asin)': 'ASIN', 'min(avg(overall))': 'Average Overall'})
display(worst_products_per_year)
ax = worst_products_per_year.plot(kind = 'scatter',x = 'Year', y = 'Average Overall',                                  title='Worst Product Overalls in Years (More Than 20 Reviews)')

plt.show()


# In[6]:


#HER YILA GÖRE EN IYI PUANA SAHİP ÜRÜNLERİN LİSTESİ VE GRAFİĞİ

adjustedDf = df.withColumn("reviewTime", to_date(col("reviewTime"), "yyyy-MM-dd"))    .withColumn('year', year("reviewTime"))

best_products_per_year = adjustedDf.groupBy("year","asin").agg(avg("overall"),count("*"))    .where("count(1) > 200").groupBy("year").agg(first("asin"),max("avg(overall)"))


best_products_per_year = best_products_per_year.toPandas()
best_products_per_year = best_products_per_year.rename(columns={'year':'Year','first(asin)': 'ASIN', 'max(avg(overall))': 'Average Overall'})
display(best_products_per_year)
ax = best_products_per_year.plot(kind = 'scatter',x = 'Year', y = 'Average Overall',                                  title='Best Product Overalls in Years (More Than 200 Reviews)')

plt.show()


# In[16]:


#- [x] her yılın mevsimlerinin en çok yorum alan ürünleri  
#her yılın mevsimlerinin en iyi review’a sahip ürünleri
#her yılın mevsimlerinin en kötü review’a sahip ürünler hesaplamalarını yapar

def get_statistics_per_season(year):    
    df.createOrReplaceTempView("TABLE")
    winter = spark.sql("SELECT * FROM TABLE where reviewTime between '"+str(year) +"-01-01' and '"+str(year) +"-03-01' ")
    spring = spark.sql("SELECT * FROM TABLE where reviewTime between '"+str(year) +"-03-01' and '"+str(year) +"-05-31' ")
    summer = spark.sql("SELECT * FROM TABLE where reviewTime between '"+str(year) +"-06-01' and '"+str(year) +"-08-31' ")
    fall = spark.sql("SELECT * FROM TABLE where reviewTime between '"+str(year) +"-09-01' and '"+str(year) +"-12-31' ")
    #Winter
    MostReviewedDF_winter = winter.groupBy("asin").count().orderBy('count', ascending=False) 
    MostReviewedDF_winter = MostReviewedDF_winter.withColumn("count", MostReviewedDF_winter["count"].cast(IntegerType()))
    listOfMostRevievewProducts_winter = MostReviewedDF_winter.take(5)
    


    i = 0
    j = 0
    x = 0
    y = 0
    print("Winter's most reviewed products on year " + str(year))
    for i in range(5):
        print(str(i+1) + ": " + getProductURL(listOfMostRevievewProducts_winter[i].asin) + " " + str(listOfMostRevievewProducts_winter[i][1])+ " reviews.")
    
    
    best_products_perSeason_winter = winter.groupBy("asin").agg(avg("overall"),count("*"))    .where("count(1) > 50").agg(first("asin"),max("avg(overall)"))
    best_products_perSeason_winter = best_products_perSeason_winter.toPandas()
    best_products_perSeason_winter = best_products_perSeason_winter.rename(columns={'first(asin)': 'ASIN', 'max(avg(overall))': 'Average Overall'})
    
    worst_products_perSeason_winter = winter.groupBy("asin").agg(avg("overall"),count("*"))    .where("count(1) > 20").agg(first("asin"),min("avg(overall)"))
    worst_products_perSeason_winter = worst_products_perSeason_winter.toPandas()
    worst_products_perSeason_winter = worst_products_perSeason_winter.rename(columns={'first(asin)': 'ASIN', 'min(avg(overall))': 'Average Overall'})
   
    print("Winter's best reviewed products on year " + str(year) + " (Over products have at least 50 reviews)")
    display(best_products_perSeason_winter)
    print("Winter's worst reviewed products on year " + str(year) + " (Over products have at least 20 reviews)")
    display(worst_products_perSeason_winter)


    #Spring
    MostReviewedDF_spring = spring.groupBy("asin").count().orderBy('count', ascending=False) 
    MostReviewedDF_spring = MostReviewedDF_spring.withColumn("count", MostReviewedDF_spring["count"].cast(IntegerType()))
    listOfMostRevievewProducts_spring = MostReviewedDF_spring.take(5)
    print("Spring's most reviewed products on year " + str(year))
    for j in range(5):
        print(str(j+1) + ": " + getProductURL(listOfMostRevievewProducts_spring[j].asin) + " " + str(listOfMostRevievewProducts_spring[j][1])+ " reviews.")
   
    best_products_perSeason_spring = spring.groupBy("asin").agg(avg("overall"),count("*"))    .where("count(1) > 50").agg(first("asin"),max("avg(overall)"))
    best_products_perSeason_spring = best_products_perSeason_spring.toPandas()
    best_products_perSeason_spring = best_products_perSeason_spring.rename(columns={'first(asin)': 'ASIN', 'max(avg(overall))': 'Average Overall'})
    
    worst_products_perSeason_spring = spring.groupBy("asin").agg(avg("overall"),count("*"))    .where("count(1) > 20").agg(first("asin"),min("avg(overall)"))
    worst_products_perSeason_spring = worst_products_perSeason_spring.toPandas()
    worst_products_perSeason_spring = worst_products_perSeason_spring.rename(columns={'first(asin)': 'ASIN', 'min(avg(overall))': 'Average Overall'})
   
    print("Spring's best reviewed products on year " + str(year) + " (Over products have at least 50 reviews)")
    display(best_products_perSeason_spring)
    print("Winter's worst reviewed products on year " + str(year) + " (Over products have at least 20 reviews)")
    display(worst_products_perSeason_spring)

    #Summer
    MostReviewedDF_summer = summer.groupBy("asin").count().orderBy('count', ascending=False) 
    MostReviewedDF_summer = MostReviewedDF_summer.withColumn("count", MostReviewedDF_summer["count"].cast(IntegerType()))
    listOfMostRevievewProducts_summer = MostReviewedDF_summer.take(5)
    print("Summer's most reviewed products on year " + str(year))
    for y in range(5):
        print(str(y+1) + ": " + getProductURL(listOfMostRevievewProducts_summer[y].asin) + " " + str(listOfMostRevievewProducts_summer[y][1])+ " reviews.")

    best_products_perSeason_summer = summer.groupBy("asin").agg(avg("overall"),count("*"))    .where("count(1) > 50").agg(first("asin"),max("avg(overall)"))
    best_products_perSeason_summer = best_products_perSeason_summer.toPandas()
    best_products_perSeason_summer = best_products_perSeason_summer.rename(columns={'first(asin)': 'ASIN', 'max(avg(overall))': 'Average Overall'})
    
    worst_products_perSeason_summer = summer.groupBy("asin").agg(avg("overall"),count("*"))    .where("count(1) > 20").agg(first("asin"),min("avg(overall)"))
    worst_products_perSeason_summer = worst_products_perSeason_summer.toPandas()
    worst_products_perSeason_summer = worst_products_perSeason_summer.rename(columns={'first(asin)': 'ASIN', 'min(avg(overall))': 'Average Overall'})
   
    print("Summer's best reviewed products on year " + str(year) + " (Over products have at least 50 reviews)")
    display(best_products_perSeason_summer)
    print("Summer's worst reviewed products on year " + str(year) + " (Over products have at least 20 reviews)")
    display(worst_products_perSeason_summer)


    
    #Fall
    MostReviewedDF_fall = fall.groupBy("asin").count().orderBy('count', ascending=False) 
    MostReviewedDF_fall = MostReviewedDF_fall.withColumn("count", MostReviewedDF_fall["count"].cast(IntegerType()))
    listOfMostRevievewProducts_fall = MostReviewedDF_fall.take(5)
    print("Fall's most reviewed products on year " + str(year))
    for x in range(5):
        print(str(j+1) + ": " + getProductURL(listOfMostRevievewProducts_fall[x].asin) + " " + str(listOfMostRevievewProducts_fall[x][1])+ " reviews.")
   

    best_products_perSeason_fall = fall.groupBy("asin").agg(avg("overall"),count("*"))    .where("count(1) > 50").agg(first("asin"),max("avg(overall)"))
    best_products_perSeason_fall = best_products_perSeason_fall.toPandas()
    best_products_perSeason_fall = best_products_perSeason_fall.rename(columns={'first(asin)': 'ASIN', 'max(avg(overall))': 'Average Overall'})
    
    worst_products_perSeason_fall = fall.groupBy("asin").agg(avg("overall"),count("*"))    .where("count(1) > 20").agg(first("asin"),min("avg(overall)"))
    worst_products_perSeason_fall = worst_products_perSeason_fall.toPandas()
    worst_products_perSeason_fall = worst_products_perSeason_fall.rename(columns={'first(asin)': 'ASIN', 'min(avg(overall))': 'Average Overall'})
   
    print("Fall's best reviewed products on year " + str(year) + " (Over products have at least 50 reviews)")
    display(best_products_perSeason_fall)
    print("Fall's worst reviewed products on year " + str(year) + " (Over products have at least 20 reviews)")
    display(worst_products_perSeason_fall)


    return

get_statistics_per_season(2016) #Parameter is changeable.


# In[8]:


#HER YILIN EN ÇOK REVIEW YAZAN KULLANICISI VE GRAFİĞİ
adjustedDf = df.withColumn("reviewTime", to_date(col("reviewTime"), "yyyy-MM-dd"))    .withColumn('year', year("reviewTime"))    .withColumn('month', month("reviewTime"))

user_with_most_reviews = adjustedDf.groupBy('year',"reviewerID").agg(count('*'))    .groupBy('year').agg(first('reviewerID'), max('count(1)'))

user_with_most_reviews = user_with_most_reviews.toPandas()
user_with_most_reviews = user_with_most_reviews.rename(columns={'year':'Year','first(reviewerID)': 'User ID', 'max(count(1))': 'Review Count'})
display(user_with_most_reviews)
ax = user_with_most_reviews.plot(kind = 'scatter',x = 'Year', y = 'Review Count',                                  title='Most Reviews by Year per User')

plt.show()


# In[10]:


#EN COK UPVOTE ALAN REVIEWLARIN INCELEME YORUMLARI
voteDF = df.filter(df.vote.isNotNull()) #Task number 14
voteDF = voteDF.withColumn("vote", voteDF["vote"].cast(IntegerType()))
listOftopReviews = voteDF.orderBy('vote', ascending=False).take(10)
topReviewTexts=[]
topReviewTextsAsin=[]

for i in range(10):
    text = listOftopReviews[i].reviewText
    asin = listOftopReviews[i].asin
    topReviewTexts.append(text)
    topReviewTextsAsin.append(asin)
    
for j in range(10):
    print(str(j+1) + ")")
    print("Product ASIN No:" + topReviewTextsAsin[j])
    print("Product URL: " + getProductURL(topReviewTextsAsin[j]))
    print(topReviewTexts[j])
    print("////////////////////////////////////////////////////")
    print("////////////////////////////////////////////////////")
    print("////////////////////////////////////////////////////")




# In[11]:


#BUTUN URUNLERIN EN COK VOTE ALAN 20 URUNUN YAZI DEGERLENDIRMESININ ORTALAMA UZUNLUGU 
listOftop20 = voteDF.orderBy('vote', ascending=False).take(20) # Task number 11
topReviewTexts20=[]


for i in range(20):
    text20 = listOftop20[i].reviewText
    topReviewTexts20.append(text20)
   
totalLen = 0

for j in range(20):
    totalLen += len(topReviewTexts20[j])

print("Average length of 20 most upvoted review text: " + str(totalLen/20) + " characters.")



# In[ ]:


#EN COK REVIEWA SAHIP URUNLERIN FOTOGRAFLI INCELEMELERI
imageDf = df.filter(df.image.isNotNull()) #Task number 12
listOfImagedProducts = imageDf.groupBy("asin").count().orderBy('count', ascending=False).take(10)
imageURLs=[]
imagedProductAsin=[]



for product in listOfImagedProducts:
    imageURL = imageDf.where("asin = '" + product.asin + "'").select("image").first()['image']
    imageURLs.append(imageURL[0])
    asin = listOftopReviews[i].asin
    imagedProductAsin.append(asin)





print("Image reviews of most reviewed products that has imaged reviews.")
for j in range (10):
    print(str(j+1) + ") " + getProductURL(imagedProductAsin[j]) )
    urllib.request.urlretrieve(imageURLs[j],"image.png")
    img = Image.open("image.png")
    img.show()

    


# In[15]:


#VERIFIED/NONVERIFIED URUNLERIN ORANI
filteredDf = df.filter(df["verified"] == "true") #Task number 13
verified_product_count = filteredDf.count()
all_product_count = df.count()
rate = (verified_product_count * 100) / all_product_count
print("Rate of verified products to all products: %" + str(rate) + ".")


# In[ ]:




