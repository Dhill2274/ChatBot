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

#Find a comment body using a parent_id key to pair a comment to the parent in the table
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
def acceptable_data(comment):
    if len(comment.split(' ')) > 50 or len(comment) < 1:
        return False
    elif len(comment) > 1000:
        return False
    elif comment == '[Deleted]' or comment == '[removed]':
        return False
    else:
        return True


if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows_counter = 0

    with open("/Users/dhillon/Documents/Code/Python/ChatBot/RC_{}".format(timeframe), buffering=1000) as dataset:
        for row in dataset:
            print(row)
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            if score >= 2:
                existing_comment_score = find_existing_score(parent_id)
                if existing_comment_score < score:
                    score = row['score']