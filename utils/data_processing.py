import pandas as pd

from tqdm.auto import tqdm

import sys
import csv

csv.field_size_limit(sys.maxsize)

def add_beginnings(df, target_column='NE-COARSE-LIT'):
    '''
    Turns starting I-labels into B-labels and returns a modified df
    '''
    all_labels = df[target_column].tolist()
    res = df.copy()
        
    for i, label in enumerate(all_labels):
        if i > 0 and label[0] == 'I' and all_labels[i-1][0] != 'I':
            res[target_column][i] = 'B'+label[1:]
            
    if all_labels[0][0] == 'I':
        res[target_column][0] = 'B'+all_labels[0][1:]
        
    return res

def df_to_sentence(df):
    '''
    Handles NoSpaceAfter and word wraps
    '''
    no_space = True
    sentence = ""
    
    tokens = df['TOKEN']
    hints = df['MISC']
    for token, hint in zip(tokens, hints):
        if token == '¬':
            no_space = True
            continue
        
        if no_space:
            sentence += token
        else:
            sentence += " " + token
        no_space = (hint == 'NoSpaceAfter')
        
    return sentence

def extract_entity_mentions(df, column='NE-COARSE-LIT'):
    '''
    Extracts entity mentions from a dataframe
    '''
    res = []
    cur_start = -1
    cur_entity = ""
    
    cnt = 0
    for token, label in zip(df['TOKEN'].tolist(), df[column].tolist()):            
        if label[0] == 'B':
            if cur_entity: 
                res.append((cur_start, cur_entity))
            cur_start = cnt
            cur_entity = token
        
        if label[0] == 'I':
            if cur_entity:
                cur_entity += ' ' + token
            else:
                print("No beginning found! Token: {0}".format(token))
                
        cnt += 1
                
    if cur_entity: 
        res.append((cur_start, cur_entity))
                      
    return res

def merge_dfs(df_list, column='NE-COARSE-LIT'):
    '''
    Merges together every two consequent sentences in a list if there is an entity mention split between them
    '''
    res = []
    cur_df = df_list[0].copy()
    for df in df_list[1:]:
        if df[column][0][0] == 'I':
            cur_df = pd.concat([cur_df, df])
        else:
            res.append(cur_df)
            cur_df = df.copy()
            
    res.append(cur_df)
    return res
              
def read_data_to_dfs_sentences(file_path):
    '''
    Returns a list of dataframes, each dataframe is a sentence (more or less)
    '''
    
    with open(file_path, 'r') as f:
        all_lines = [line.strip('\n') for line in f.readlines()]
        
    docs = []
    col_names = list(pd.read_table(file_path, engine='python', quoting=csv.QUOTE_NONE, nrows=1).columns)
    cur_doc = pd.DataFrame(columns=col_names)
    
    cur_sentence_finished = False
    
    for line in tqdm(all_lines[1:]):
        if len(line) == 0:
            continue
            
        if line.startswith("# document"):
            docs.append(cur_doc)
            cur_doc = pd.DataFrame(columns=col_names)
            cur_sentence_finished = False
            
        if line.startswith("# segment") and cur_sentence_finished:
            docs.append(cur_doc)
            cur_doc = pd.DataFrame(columns=col_names)
            cur_sentence_finished = False

        if line[0] != '#':
            items = line.split('\t')
            cur_doc = cur_doc.append(pd.Series(items,index = col_names), ignore_index = True)
    
            if len(cur_doc) > 1:
                last_token = cur_doc['TOKEN'].tolist()[-1]
                prev_token = cur_doc['TOKEN'].tolist()[-2]
                if last_token in ['.', '!', '?'] and not prev_token in ['Mr', 'Mrs', 'Dr']:
                    cur_sentence_finished = True
            
    if cur_doc is not None and not cur_doc.empty:
        docs.append(cur_doc) 
        
    return docs    
    
    
def write_results(df_list, filename='results.tsv'):
    '''
    Writes dataframes to file, separating with "#"
    '''
    with open(filename, 'w') as f:
        f.write("\t".join(df_list[0].columns.tolist()) + '\n')
                
    for df in df_list:
        df.to_csv(filename, mode='a', sep='\t', quoting=csv.QUOTE_NONE, header=False, index=False)
        with open(filename, 'a') as f:
            f.write("#\n")
            
            
###    
''' Old things kept just in case start here: '''

def read_data_to_docs(file_path, MIN_FRAGMENT_LENGTH=10):
    '''
    Returns a list of documents
    Each document is a list of dataframes, each dataframe representing a document fragment
    '''
    
    with open(file_path, 'r') as f:
        all_lines = [line.strip('\n') for line in f.readlines()]
        
    docs = []
    fragments = []

    col_names = list(pd.read_table(file_path, engine='python', quoting=csv.QUOTE_NONE, nrows=1).columns)
    cur_fragment = pd.DataFrame(columns=col_names)
    
    for line in all_lines[1:]:
        if len(line) == 0:
            continue
                        
        if line.startswith("# document"):
            if not cur_fragment.empty:
                fragments.append(cur_fragment)
             
            if len(fragments) > 0:
                docs.append(fragments)
                
            fragments = []
            cur_fragment = pd.DataFrame(columns=col_names)
                
        if line.startswith("# segment"): # and len(cur_fragment) >= MIN_FRAGMENT_LENGTH:
            fragments.append(cur_fragment)
            cur_fragment = pd.DataFrame(columns=col_names)

        if line[0] != '#':
            items = line.split('\t')
            cur_fragment = cur_fragment.append(pd.Series(items,index = col_names), ignore_index = True)
               
            
    if not cur_fragment.empty:
        fragments.append(cur_fragment)    
    if fragments:
        docs.append(fragments)
        
    return docs
    
def write_docs(docs_list, filename='results.tsv'):
    '''
    Writes list of documents, each document is a list of dataframes
    '''
    with open(filename, 'w') as f:
        f.write("\t".join(docs_list[0][0].columns.tolist()) + '\n')
     
    for doc in docs_list:
        for df in doc:
            df.to_csv(filename, mode='a', sep='\t', quoting=csv.QUOTE_NONE, header=False, index=False)
            time.sleep(0.1)
            with open(filename, 'a') as f:
                f.write("#\n")
                
        with open(filename, 'a') as f:
            f.write("\n")  
            

def read_data_to_dfs(file_path, MIN_DOC_LENGTH=10):
    '''
    Returns a list of dataframes, each representing a document fragment not shorter than MIN_DOC_LENGTH
    '''
    
    with open(file_path, 'r') as f:
        all_lines = [line.strip('\n') for line in f.readlines()]
        
    docs = []
    col_names = list(pd.read_table(file_path, engine='python', quoting=csv.QUOTE_NONE, nrows=1).columns)
    cur_doc = pd.DataFrame(columns=col_names)
    
    for line in all_lines[1:]:
        if len(line) == 0:
            continue
            
        if line.startswith("# document") and len(cur_doc) > 0:
            docs.append(cur_doc)
            cur_doc = pd.DataFrame(columns=col_names)
            
        if line.startswith("# segment") and len(cur_doc) >= MIN_DOC_LENGTH:
            docs.append(cur_doc)
            cur_doc = pd.DataFrame(columns=col_names)

        if line[0] != '#':
            items = line.split('\t')
            cur_doc = cur_doc.append(pd.Series(items,index = col_names), ignore_index = True)
            
    if cur_doc is not None and not cur_doc.empty:
        docs.append(cur_doc) 
        
    return docs