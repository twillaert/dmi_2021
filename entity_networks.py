'''
Code for the paper on narrative convergence in instagram conspiracies
Co-occurences of named entities
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

def get_NER_network(snapshot, entity_list, full_df):

    '''
    get co-occurence network of listed entities in a dataframe of instagram posts
    takes dataframe of instagram posts, list of entities to retrieve
    returns gexf file of networked entities
    '''
    G = nx.MultiGraph()
    G.add_nodes_from(entity_list)

    filename = str(snapshot['quarter'].iloc[0])
    print('getting network for: ' + filename)

    weight = 1.0 #default weight = 1
    for text in snapshot['body']: 
        text = clean_instagram_post(text)
        entity_matches = find_entities(text, entity_list)
        entity_matches = list(dict.fromkeys(entity_matches)) 
        edges = combinations(entity_matches, 2) #get tuples entity matches
        weighted_edges = []
        for edge in edges: 
            edge_list = list(edge)
            edge_list.append(weight)
            weighted_edges.append((tuple(edge_list)))

        G.add_weighted_edges_from(weighted_edges) #add weighted edges

    for row in combined_df.iterrows():
        try:
            G.nodes[row[1]['Entities']]['type'] = row[1]['type']
            G.nodes[row[1]['Entities']]['category'] = row[1]['Categories']
        except KeyError:
            print(f"No node named {row[1]['Entities']} found, skipping...")
        
    nx.write_gexf(G, filename + '_PERSONS_ORGANIZATIONS_weighted.gexf')

#read the sql database, store as dataframe
con = sqlite3.connect('Fabio_insta.sqlite')
posts_df = pd.read_sql_query("SELECT timestamp, body FROM insta_posts", con)
con.close()

#convert dataframe unix timestamps to datetime
posts_df['timestamp'] = pd.to_datetime(posts_df['timestamp'], unit='s')

#define entity list
entity_df1 = pd.read_csv("Instagram_NER - ORGANIZATIONS_CLEAN.csv")[['Organization', 'Categories']]
entity_list1 = entity_df1['Organization']
entity_list1 = [entity.lower().strip() for entity in entity_list1]
entity_list1 = list(dict.fromkeys(entity_list1)) 

entity_df = pd.read_csv("Instagram_NER - PERSONS_CLEAN.csv")[['Person', 'Categories']]
entity_list = entity_df["Person"]
entity_list = [entity.lower().strip() for entity in entity_list]
entity_list = list(dict.fromkeys(entity_list)) 

entity_list_all = list(dict.fromkeys(entity_list + entity_list1))

# Build combined dataframe: Rename 'Organization' and 'Person' columns to 'Entities', add new column 'type' to
# retain the information, then combine dataframes
entity_df1.rename({'Organization': 'Entities'}, axis=1, inplace=True)
entity_df1['type'] = 'organizaion'
entity_df.rename({'Person': 'Entities'}, axis=1, inplace=True)
entity_df['type'] = 'person'
combined_df = pd.concat([entity_df1, entity_df])
combined_df['Entities'] = combined_df['Entities'].str.lower()

#look only at data for 2020, identify quarters
posts_df = posts_df[posts_df['timestamp'].dt.year == 2020]
posts_df['quarter'] = pd.PeriodIndex(posts_df.timestamp, freq='Q')

#group the data by quarter, get network per quarter
for name, group in posts_df.groupby('quarter'):
    get_NER_network(group, entity_list_all, combined_df)
