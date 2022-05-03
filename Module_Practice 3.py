#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json 
import numpy as np
import warnings
import re
warnings.filterwarnings('ignore')


# In[2]:


file_dir = 'C:/Users/14153/UCDavis_Code/Bootcamp/ETL/Kaggle/'


# In[3]:


kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
ratings = pd.read_csv(f'{file_dir}ratings.csv')


# In[4]:


kaggle_metadata.head(5)


# In[5]:


kaggle_metadata.sample(n=5)


# In[6]:


kaggle_metadata.tail(5)


# In[7]:


ratings.head(5)


# In[8]:


ratings.sample(5)


# In[9]:


ratings.tail(5)


# ## 8.3.1 Data Cleaning Strategies

# In[10]:


with open(f'{file_dir}wikipedia_movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)


# In[11]:


len(wiki_movies_raw)


# In[12]:


wiki_movies_raw[:5]


# In[13]:


wiki_movies_raw[-5:]


# In[14]:


wiki_movies_raw[3600:3605]


# In[15]:


wiki_movies_df = pd.DataFrame(wiki_movies_raw)
wiki_movies_df.head(5)


# In[16]:


columns_list = wiki_movies_df.columns.tolist()
columns_list 


# In[17]:


###List Comprehension to Filter to only the movies wtih a director and IMBD link 
wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie]
len(wiki_movies)


# ## 8.3.5 Create a Function to Clean Data 

# In[18]:


wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]


# In[19]:


def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    return movie


# In[20]:


wiki_movies_df[wiki_movies_df['Arabic'].notnull()]


# In[21]:


wiki_movies_df[wiki_movies_df['Arabic'].notnull()]['url']


# In[22]:


sorted(wiki_movies_df.columns.tolist())


# In[23]:


def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    return movie


# In[24]:


def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:

            return movie


# In[25]:


def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:

            return movie


# In[26]:


def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)


    return movie


# In[27]:


def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    return movie


# In[28]:


clean_movies = [clean_movie(movie) for movie in wiki_movies]


# In[29]:


wiki_movies_df = pd.DataFrame(clean_movies)
sorted(wiki_movies_df.columns.tolist())


# In[30]:


def change_column_name(old_name, new_name):
    if old_name in movie:
        movie[new_name] = movie.pop(old_name)


# In[31]:


# Add a nested change_column_name function to the clean_movie function
def clean_movie(movie):
   movie = dict(movie) # Create a non-destructive copy
   alt_titles = {}
   # Combine alternate titles into one list
   for key in ['Also known as','Arabic','Cantonese','Chinese','French',
               'Hangul','Hebrew','Hepburn','Japanese','Literally',
               'Mandarin','McCune-Reischauer','Original title','Polish',
               'Revised Romanization','Romanized','Russian',
               'Simplified','Traditional','Yiddish']:
       if key in movie:
           alt_titles[key] = movie[key]
           movie.pop(key)
   if len(alt_titles) > 0:
       movie['alt_titles'] = alt_titles
   # Merge column names
   def change_column_name(old_name, new_name):
       if old_name in movie:
           movie[new_name] = movie.pop(old_name)
   change_column_name('Adaptation by', 'Writer(s)')
   change_column_name('Country of origin', 'Country')
   change_column_name('Directed by', 'Director')
   change_column_name('Distributed by', 'Distributor')
   change_column_name('Edited by', 'Editor(s)')
   change_column_name('Length', 'Running time')
   change_column_name('Original release', 'Release date')
   change_column_name('Music by', 'Composer(s)')
   change_column_name('Produced by', 'Producer(s)')
   change_column_name('Producer', 'Producer(s)')
   change_column_name('Productioncompanies ', 'Production company(s)')
   change_column_name('Productioncompany ', 'Production company(s)')
   change_column_name('Released', 'Release Date')
   change_column_name('Release Date', 'Release date')
   change_column_name('Screen story by', 'Writer(s)')
   change_column_name('Screenplay by', 'Writer(s)')
   change_column_name('Story by', 'Writer(s)')
   change_column_name('Theme music composer', 'Composer(s)')
   change_column_name('Written by', 'Writer(s)')
   return movie


# In[32]:


clean_movies = [clean_movie(movie) for movie in wiki_movies]
wiki_movies_df = pd.DataFrame(clean_movies)
sorted(wiki_movies_df.columns.tolist())


# In[33]:


wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')


# In[34]:


wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
print(len(wiki_movies_df))
wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
print(len(wiki_movies_df))
wiki_movies_df.head()


# In[35]:


[[column,wiki_movies_df[column].isnull().sum()] for column in wiki_movies_df.columns]


# In[36]:


[column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]


# In[37]:


wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]


# In[38]:


box_office = wiki_movies_df['Box office'].dropna()


# In[39]:


def is_not_a_string(x):
    return type(x) != str


# In[40]:


box_office[box_office.map(is_not_a_string)]


# In[41]:


lambda arguments: expression


# In[42]:


lambda x: type(x) != str


# In[43]:


def is_not_a_string(x):
    return type(x) != str


# In[44]:


box_office[box_office.map(is_not_a_string)]


# In[45]:


box_office[box_office.map(lambda x: type(x) != str)]


# In[52]:


some_list = ['One','Two','Three']
'Mississippi'.join(some_list)


# In[53]:


box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)


# In[54]:


form_one = r'\$\d+\.?\d*\s*[mb]illion'


# In[55]:


box_office.str.contains(form_one, flags=re.IGNORECASE, na=False).sum()


# In[56]:


form_two = r'\$\d{1,3}(?:,\d{3})+'
box_office.str.contains(form_two, flags=re.IGNORECASE, na=False).sum()


# In[57]:


matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE, na=False)
matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE, na=False)


# In[58]:


form_one = r'\$\d+\.?\d*\s*[mb]illion'


# In[59]:


box_office.str.contains(form_one, flags=re.IGNORECASE, na=False).sum()


# In[60]:


form_two = r'\$\d{1,3}(?:,\d{3})+'
box_office.str.contains(form_two, flags=re.IGNORECASE, na=False).sum()


# In[61]:


matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE, na=False)
matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE, na=False)


# In[62]:


box_office[~matches_form_one & ~matches_form_two]


# In[63]:


form_one = r'\$\s*\d+\.?\d*\s*[mb]illion'
form_two = r'\$\s*\d{1,3}(?:,\d{3})+'


# In[64]:


form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+'


# In[65]:


form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'


# In[66]:


box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)


# In[67]:


form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'


# In[68]:


box_office.str.extract(f'({form_one}|{form_two})')


# In[71]:


def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        # return value
        return value

    # otherwise, return NaN
    else:
        return np.nan


# In[72]:


wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)


# In[73]:


wiki_movies_df.drop('Box office', axis=1, inplace=True)


# In[74]:


budget = wiki_movies_df['Budget'].dropna()


# In[75]:


budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)


# In[76]:


budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)


# In[77]:


matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE, na=False)
matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE, na=False)
budget[~matches_form_one & ~matches_form_two]


# In[78]:


budget = budget.str.replace(r'\[\d+\]\s*', '')
budget[~matches_form_one & ~matches_form_two]


# In[79]:


wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)


# In[80]:


wiki_movies_df.drop('Budget', axis=1, inplace=True)


# In[81]:


release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)


# In[82]:


date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]?\d,\s\d{4}'
date_form_two = r'\d{4}.[01]\d.[0123]\d'
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
date_form_four = r'\d{4}'


# In[83]:


release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)


# In[84]:


wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)


# In[85]:


running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)


# In[86]:


running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE, na=False).sum()


# In[87]:


running_time[running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE, na=False) != True]


# In[88]:


running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE, na=False).sum()


# In[89]:


running_time[running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE, na=False) != True]


# In[90]:


running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')


# In[91]:


running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)


# In[92]:


wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)


# In[93]:


wiki_movies_df.drop('Running time', axis=1, inplace=True)


# In[ ]:




