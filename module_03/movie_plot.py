import matplotlib
matplotlib.rcParams['font.family']
import matplotlib.font_manager as fm
import pandas as pd
import numpy as np
import pymysql
import matplotlib.pyplot as plt


font_name = fm.FontProperties(fname = 'C:/Windows/Fonts/malgun.ttf').get_name()
matplotlib.rc('font', family = font_name)
matplotlib.rcParams['axes.unicode_minus'] = False


conn = pymysql.connect(host = 'skuser57-instance.ctxvewe48g71.us-west-2.rds.amazonaws.com',
port = 3306, user = 'admin', password = 'pax5022kr', db = 'mydb')
conn_stock = pymysql.connect(host = 'skuser57-instance.ctxvewe48g71.us-west-2.rds.amazonaws.com',port = 3306, user = 'admin', password = 'pax5022kr', db = 'mydb')

curs = conn.cursor()
curs_stock = conn_stock.cursor()


sql = "SELECT * FROM my_topic_movie"
# sql_stock = "SELECT * FROM topic_stock"

curs.execute(sql)
# curs_stock.execute(sql_stock)

rows = curs.fetchall()
# rows_stock = curs_stock.fetchall()


df  = pd.read_sql(sql, conn)
# df_stock  = pd.read_sql(sql_stock, conn_stock)


def change_money(data):
    data = data.replace("$","")
    if data[-1] == 'M':
        data = data.replace("M","")
        data = float(data)*1000000
    elif data[-1] == 'K':
        data = data.replace("K","")
        data = float(data)*1000
    return int(data)

def change_people(data):
    return float(data.replace(",",""))






# jongmin
x = np.arange(df['title'].count())
weeks = df["weeks"]
title  = df["title"]
plt.bar(x, weeks)
plt.xticks(x,title ,rotation=90)
plt.xlabel('title', fontsize=14)
plt.ylabel('weeks', fontsize=14)
# plt.show()
plt.savefig('movie_05.png')




# df1 = df.sort_values(by=['rating'], axis=0, ascending=True)
title = df['title']
rating = df['rating']
plt.bar(x, rating, color = 'blue')
plt.xlabel('title', fontsize=14)
plt.xticks(x,title ,rotation=90)
plt.ylabel('rating',fontsize=14)
# plt.show()
plt.savefig('movie_03.png')
# # # 주말동안 번 돈





# # # jiho
df['weekend'] = df['weekend'].apply(change_money)
df = df.sort_values(by=['weekend'], axis=0, ascending=False)
fig = plt.figure()
ax = fig.add_axes([0.1, 0.3, 0.8, 0.6])
ax.plot(df['title'],df['weekend'])
ax.set_xlabel('title')
ax.set_ylabel('weekend gross')
ax.set_title('movie weekend gross')
plt.xticks(rotation=45)
# plt.show()
plt.savefig('movie_01.png')

# # jiho
df['gross'] = df['gross'].apply(change_money)
df = df.sort_values(by=['gross'], axis=0, ascending=False)
fig = plt.figure()
ax = fig.add_axes([0.1, 0.3, 0.8, 0.6])
ax.plot(df['title'],df['gross'])
ax.set_xlabel('title')
ax.set_ylabel('total gross')
ax.set_title('movie total gross')
plt.xticks(rotation=45)
# plt.show()
plt.savefig('movie_02.png')
# # print(df)




# # jiho
df['people'] = df['people'].apply(change_people)
df = df.sort_values(by=['people'], axis=0, ascending=False)
fig = plt.figure()
ax = fig.add_axes([0.1, 0.3, 0.8, 0.6])
ax.plot(df['title'],df['people'])
ax.set_xlabel('title', fontsize=14)
ax.set_ylabel('people', fontsize=14)
ax.set_title('movie people')
plt.xticks(rotation=45)
# plt.show()
plt.savefig('movie_04.png')



conn.close()


# USE mydb;
# CREATE TABLE movie(
# id INT AUTO_INCREMENT PRIMARY KEY,
# title VARCHAR(20)  NOT NULL,
# weekend VARCHAR(20)NOT NULL,
# gross VARCHAR(20)NOT NULL,
# weeks VARCHAR(20)NOT NULL,
# genre VARCHAR(100) NOT NULL,
# rating VARCHAR(10) NOT NULL,
# movie_release VARCHAR(20) NOT NULL,
# people VARCHAR(20) NOT NULL,
# created_at DATETIME DEFAULT NOW()
# );

# INSERT INTO movie(title,weekend,gross,weeks,genre,movie_release,rating,people,created_at) VALUES('c','c','c','c','c','c','c','c',NOW());