'''
Code for the paper on narrative convergence in instagram conspiracies
Co-occurences of named entities
Tom Willaert, VUB AI LAB / DMI summerschool 2021
'''

import sqlite3
import pandas as pd
import re
import networkx as nx
from itertools import combinations

#define helper functions

def clean_instagram_post(text):
    text = re.sub(r"@(\w+)", '', text) #remove @-handles
    text = re.sub(r"#(\w+)", '', text) #remove hashtags
    text = text.lower() #make lowercase
    return text


def find_entities(text, list_of_entities):
    #convert list of entities to single regex query
    query = '(?:% s)' % '|'.join(list_of_entities)
    return re.findall(query, text)
    

def get_NER_network(snapshot, entity_list):

    '''
    get co-occurence network of listed entities in a dataframe of instagram posts
    takes dataframe of instagram posts, list of entities to retrieve
    returns graphml file of networked entities
    '''
    G = nx.Graph()
    G.add_nodes_from(entity_list)

    filename = str(snapshot['quarter'].iloc[0])
    print('getting network for: ' + filename)

    for text in snapshot['body']: 
        text = clean_instagram_post(text)
        entity_matches = find_entities(text, entity_list)
        entity_matches = list(dict.fromkeys(entity_matches)) 
        edges = combinations(entity_matches, 2) #get tuples entity matches
        G.add_edges_from(edges)
        
    nx.write_graphml(G, filename + '_PERSONS_ORGANIZATIONS.graphml')

#read the sql database, store as dataframe
con = sqlite3.connect('Fabio_insta.sqlite')
posts_df = pd.read_sql_query("SELECT timestamp, body FROM insta_posts", con)
con.close()

#convert dataframe unix timestamps to datetime
posts_df['timestamp'] = pd.to_datetime(posts_df['timestamp'], unit='s')

#define entity list
entity_list1 = pd.read_excel('organizations_cleaned_school.xlsx')['Organization']
entity_list1 = [entity.lower().strip() for entity in entity_list1]
entity_list1 = list(dict.fromkeys(entity_list1)) 

entity_list = pd.read_excel('persons_cleaned_school.xlsx')['Person']
entity_list = [entity.lower().strip() for entity in entity_list]
entity_list = list(dict.fromkeys(entity_list)) 

entity_list_all = list(dict.fromkeys(entity_list + entity_list1))

#look only at data for 2020, identify quarters
posts_df = posts_df[posts_df['timestamp'].dt.year == 2020]
posts_df['quarter'] = pd.PeriodIndex(posts_df.timestamp, freq='Q')

#group the data by quarter, get network per quarter
for name, group in posts_df.groupby('quarter'):
    get_NER_network(group, entity_list_all)
    

