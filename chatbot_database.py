import sqlite3
import json
from datetime import datetime

sql_transaction = []
timeframe = '2015-01'

#Create our SQL data base to store our parsed Reddit conversation data
connection = sqlite3.connect('{}.db'.format(timeframe))
cursor = connection.cursor()

def create_table():
    cursor.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

#Replace all '/n' characters with a string to standardise data
def format_data(data):
    data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ')
    return data

#Find score of the id of the comment under the parent
def find_existing_score(parent_id):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(parent_id)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False

#Find a body of parent using a parent_id key to pair a parent body to the row of the table
def find_parent(parent_id):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(parent_id)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False
    
#Return true for comment of set length and that isn't deleted
def acceptable_data(body):
    if len(body.split(' ')) > 50 or len(body) < 1:
        return False
    elif len(body) > 1000:
        return False
    elif body == '[deleted]' or body == '[removed]':
        return False
    else:
        return True
    
def sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score):
    try:
        sql = "UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;".format(parent_id, comment_id, parent_data, body, subreddit, int(created_utc), score, parent_id)
        transaction_bldr(sql)
    except Exception as e:
        print('replace comment', e)

def sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score):
    try:
        sql = 'INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});'.format(parent_id, comment_id, parent_data, body, subreddit, int(created_utc), score)
        transaction_bldr(sql)
    except Exception as e:
        print('update params',str(e))

def sql_insert_no_parent(comment_id,parent_id,body,subreddit,created_utc, score):
    try:
        sql = 'INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});'.format(parent_id, comment_id, body, subreddit, int(created_utc), score)
        transaction_bldr(sql)
    except Exception as e:
        print('update params',str(e))

def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        cursor.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                cursor.execute(s)
            except:
                pass
            connection.commit()
            sql_transaction = []

if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows_counter = 0

    with open("/Users/dhillon/Documents/Code/Python/ChatBot/RC_{}".format(timeframe), buffering=1000) as dataset:
        for row in dataset:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            comment_id = row['name']
            parent_data = find_parent(parent_id)

            if score >= 2:
                if acceptable_data(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if existing_comment_score < score:
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows_counter += 1
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
            
            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows_counter, str(datetime.now())))