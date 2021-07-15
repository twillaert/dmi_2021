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

def clean_df(df: pd.DataFrame):
    # Make lowercase and remove trailing whitespace
    df['Entities'] = df['Entities'].str.lower().str.strip()
    # Remove newlines
    df['Entities'] = df['Entities'].replace(r'\\n', ' ', regex=True)
    return df


def create_entity_list(df: pd.DataFrame, entity_type_column: str):
    """Create entity list from dataframe and lowercase entity names."""
    entity_list = df[entity_type_column]
    entity_list = [entity.lower().strip() for entity in entity_list]
    return list(dict.fromkeys(entity_list))


def normalize_df(df: pd.DataFrame, entity_type_column: str):
    """Prepare merging of dataframes by applying consistent format to each.

    Rename 'Organization', 'Person' or 'Conspiracy' columns to 'Entities' and 
    add new column 'type' to retain information about entity type.
    """
    df.rename({entity_type_column: 'Entities'}, axis=1, inplace=True)
    df['type'] = entity_type_column
    return df


def clean_instagram_post(text: str):
    text = re.sub(r"@(\w+)", '', text) #remove @-handles
    text = re.sub(r"#(\w+)", '', text) #remove hashtags
    text = text.lower() #make lowercase
    return text


def find_entities(text: str, list_of_entities: list[str]):
    # convert list of entities to single regex query
    query_list = []
    for entity in list_of_entities:
        query_list.append(rf"\b{entity}\b")
    return re.findall('|'.join(query_list), text)


def get_NER_network(snapshot: pd.DataFrame, entity_list: list[str], full_df: pd.DataFrame):

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

    # Add node attributes: Type and category of the entity the node represents
    for row in combined_df.iterrows():
        try:
            G.nodes[row[1]['Entities']]['type'] = row[1]['type']
            G.nodes[row[1]['Entities']]['category'] = row[1]['Categories']
        except KeyError:
            print(f"No node named {row[1]['Entities']} found, skipping...")
        
    nx.write_gexf(G, filename + '_PERSONS_ORGANIZATIONS_CONSPIRACIES_weighted.gexf')


#read the sql database, store as dataframe
con = sqlite3.connect('Fabio_insta.sqlite')
posts_df = pd.read_sql_query("SELECT timestamp, body FROM insta_posts", con)
con.close()

#convert dataframe unix timestamps to datetime
posts_df['timestamp'] = pd.to_datetime(posts_df['timestamp'], unit='s')

#create dataframes and entity lists
df_orgs = pd.read_csv("Instagram_NER - ORGANIZATIONS_CLEAN.csv")[['Organization', 'Categories']]
org_list = create_entity_list(df_orgs, 'Organization')

df_person = pd.read_csv("Instagram_NER - PERSONS_CLEAN.csv")[['Person', 'Categories']]
person_list = create_entity_list(df_person, 'Person')

df_conspiracies = pd.read_csv("Instagram_NER - CONSPIRACIES_NAMES.csv")[['Conspiracy', 'Categories']]
conspiracies_list = create_entity_list(df_conspiracies, 'Conspiracy')

entity_list_all = list(dict.fromkeys(org_list + person_list + conspiracies_list))

# Normalize, combine and clean dataframes. The resulting dataframe is used to add node attributes
# in get_NER_network()
df_orgs = normalize_df(df_orgs, 'Organization')
df_person = normalize_df(df_person, 'Person')
df_conspiracies = normalize_df(df_conspiracies, 'Conspiracy')
combined_df = pd.concat([df_orgs, df_person, df_conspiracies])
combined_df = clean_df(combined_df)

#look only at data for 2020, identify quarters
posts_df = posts_df[posts_df['timestamp'].dt.year == 2020]
posts_df['quarter'] = pd.PeriodIndex(posts_df.timestamp, freq='Q')

#group the data by quarter, get network per quarter
for name, group in posts_df.groupby('quarter'):
    get_NER_network(group, entity_list_all, combined_df)
