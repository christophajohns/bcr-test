# -*- coding: utf-8 -*-
# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Testing the Brandwatch Consumer Research Python Client Library
# 
# ## Introduction
# 
# The goal of this notebook is to test the Python Client Library for the Brandwatch Consumer Research API to achieve the same result as with the Crimson Hexagon API.
# %% [markdown]
# ## General setup

# %%
import logging
from ratelimit import limits, sleep_and_retry
from functools import partial

logger = logging.getLogger("bcr_api")
TEN_MINUTES = 600 # 10 minutes in seconds
start_date = "2012-01-01"

@sleep_and_retry
@limits(calls=8, period=TEN_MINUTES)
def call_api(func):
    return func()

# %% [markdown]
# ## Authenticate

# %%
from bcr_api.bwproject import BWUser
import os

USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')
TOKEN_PATH = 'tokens.txt'

BWUser(username=USERNAME, password=PASSWORD, token_path=TOKEN_PATH)

# %% [markdown]
# ## Get project

# %%
from bcr_api.bwproject import BWProject

PROJECT = 'Universität Göttingen'

project = BWProject(username=USERNAME, project=PROJECT)

# %% [markdown]
# ## Add queries
# %% [markdown]
# ### Calculate queries

# %%
import itertools

def get_brand_combinations(brands_list):
    """Erstellt Liste von Listen von Dictionaries für die Marken-Schnittmengen
        Die ausgebene Liste hat folgende Struktur:
        brand_combinations = [
            [
                {
                    "name": "FantasticBrand",
                    "short": "1",
                    "keywords": "FantasticBrand OR 'Fantastic Brand' OR ..."
                },
            ],
            [
                {
                    "name": "SuperBrand",
                    "short": "2",
                    "keywords": "SuperBrand OR 'Super Brand' OR ..."
                },
            ],
            [
                {
                    "name": "FantasticBrand",
                    "short": "1",
                    "keywords": "FantasticBrand OR 'Fantastic Brand' OR ..."
                },
                {
                    "name": "SuperBrand",
                    "short": "2",
                    "keywords": "SuperBrand OR 'Super Brand' OR ..."
                },
            ],
        ]

    Parameters:
        brands_list (list(dict)): Liste der Dictionaries, die alle Informationen zu den jeweiligen Marken enthalten

    Returns:
        brand_combinations: Liste mit allen Kombinationen dieser Dictionaries als Listen (Struktur s.o.)
    """
    brand_combinations = []
    for L in range(1, len(brands_list)+1):
        for subset in itertools.combinations(brands_list, L):
            brand_combinations.append({'brands': list(subset)})
    return brand_combinations


companies = [
    {
        "company_name": "American Eagle Outfitters",
        "ticker": "AEO",
        "brands": [
            {
                "name": "American Eagle",
                "short": "1",
                "keywords": """("American Eagle" OR AmericanEagl* OR @AEO OR @aeo_ph OR #Aeo_Ph OR #AmericanEagleOut*) AND -("Envoy Air" OR envoy* OR Airlin* OR Airfare* OR AmericanAirlin* OR "Eagle Gold" OR "Gold Eagle" OR "Gold Eagles" OR GoldEagl* OR EagleGold* OR AmericanGoldEagl* OR AmericanEagleGold* OR AmericanEaglesGold* OR Coin OR Coins OR NGC* OR "Silver Eagle" OR "Silver Eagles" OR "Silver Dollar" OR "Silver Dollars" OR "Silver American" OR APMEX* OR "oz fine" OR SilverEagle* OR SilverDollar* OR SilverAmerican* OR AmericanSilver* OR "Eagle Instrument" OR "Eagle Instruments" OR "Eagles Instrument" OR "Eagles Instruments" OR Dental OR Dentis* OR americaneagleinstrum* OR americaneaglesinstrum* OR "Eagles Bank" OR "Eagle Bank" OR AmericanEaglesBank* OR AmericanEagleBank* OR "Desert Eagle" OR "Desert Eagles" OR Deserteagle* OR "Magnum Research" OR MagnumResearch* OR "Eagle Firearms" OR "Eagles Firearm" OR "Eagles Firearms" OR "Eagle Firearm" OR EagleFirearm* OR EaglesFirearm* OR AmericanEagleFirearm* OR AmericanEaglesFirearm* OR Ammunition* OR "Food Machinery" OR Foodmachin* OR AmericanEagleFood* OR AmericanEaglesFood* OR "Bald Eagle" OR "Bald Eagles" OR Baldeagl* OR AEF OR @AEF* OR #AEF* OR "Eagle Foundation" OR "Eagles Foundation" OR "Eagle Foundations" OR "Eagles Foundations" OR Eaglefoundat* OR Eaglesfoundat* OR AmericanEaglefoundat* OR AmericanEaglesfoundat* OR "Eagle Manufacturing" OR "Eagles Manufacturing" OR Eaglemanufact* OR Eaglesmanufact* OR AmericanEaglemanufact* OR AmericanEaglesmanufact* OR "land speed record" OR landspeed* OR "North American Eagle" OR "Northamerican Eagle" OR "Northamerican Eagles" OR Northamericaneagl* OR "Credit Union" OR AECU OR "U.S. Eagle" OR "U.S.Eagle" OR useagle* OR federaleagle* OR "Federal Eagle" OR @AE_MTB OR #AE_MTB OR AmEagleCU OR @AmEagleCU OR #AmEagleCU OR @ameagleinst OR #ameagleinst)"""
            },
            {
                "name": "Aerie",
                "short": "2",
                "keywords": """(Aerie OR @Aerie OR #Aerie) AND -(AeriePharma* OR Pharma* OR theaeriat* OR @theaerieat* OR #@theaerieat* OR "Eagle Landing" OR "Eagles Landing" OR "Eagles Landings" OR "Eagle Landings" OR EagleLanding* OR EaglesLanding* OR "Wedding Venue" OR "Wedding Venues" OR weddingvenue OR "Weddings Venue" OR "Weddings Venues" OR weddingsvenue* OR "Aerie Consulting" OR AerieConsult* OR @AerieConsult* OR #AeriConsult* OR "Aerie Class" OR "Aerie Classes" OR aerie-class* OR Aerieclass* OR "Star Trek" OR StarTrek OR "Aerie Vessel" OR "Aerie Vessels" OR AerieVess* OR "Aerie Maganzine" OR AerieMag* OR DargonAeri* OR "Dragon Aerie" OR "Dargons Aerie" OR DragonsAeri* OR "Dark Soul" OR "Dark Souls" OR DarkSoul* OR Baldur* OR Avariel* OR (Mercedes* AND Lackey*) OR Jouster* OR (Thoma* AND Sniegoski*) OR "John Denver" OR JohnDenver* OR "Denver John" OR DenverJohn* OR (Jefferson* AND Airplane*) OR "Gang of Eagle" OR GangofEagle* OR (Long AND John AND Silver)OR LongJohn* OR (Jazz AND Ingo*) OR (Jazz AND HippIng*) OR Birdnest* OR Birdsnest* OR (Bird* AND Nest*) OR FraternalOrder* OR "Fraternal Order" OR BirdOfPrey* OR @BirdOfPrey* OR #BirdofPrey* OR @OfficialAerie* OR #OfficialAerie* OR @AerieYearbook* OR #AerieYearbook* OR @aerielgrc* OR @aerietri* OR #aerielgrc* OR @aerietri* OR @FOEGrandAerie* OR #FOEGrandAerie* OR @crashaerien* OR #crashaerien* OR @CulinAerie* OR #CulinAerie* OR @aerielash* OR #aerielash* OR @trustfund* OR #trustfund*)"""
            }
        ]
    }
]

for company in companies:
    company['brand_combinations'] = get_brand_combinations(brands_list=company['brands'])
    print('Number of combinations: {}'.format(len(company['brand_combinations'])))
    print('Example combination:')
    print(company['brand_combinations'][0])


# %%
def get_brand_combi_keywords(brand_combi):
    """Verknüpft die Suchanfragen der einzelnen Marken mit UND-Operator

    Parameters:
        brand_combi (list(dict)): Liste der Dictionaries, die die Informationen zu den Marken dieser Schnittmenge
            enthalten

    Returns:
        str: UND-verknüpfte Suchanfrage für die Schnittmenge der Marken
    """
    keywords_list = [brand["keywords"] for brand in brand_combi['brands']]
    keywords = "(" + ") AND (".join(keywords_list) + ")"
    return keywords


for company in companies:
    for brand_combi in company['brand_combinations']:
        brand_combi['keywords'] = get_brand_combi_keywords(brand_combi=brand_combi)
    print('Example combination:')
    print(company['brand_combinations'][0]['keywords'])


# %%
def get_brand_combi_name(brand_combi):
    """Ermittelt den Namen für die Kombinations-Query aus den "short"-Bezeichnungen der Marken

    Parameters:
        brand_combi (list(dict)): Liste der Dictionaries, die die Informationen zu den Marken dieser Schnittmenge
            enthalten

    Returns:
        str: Name für diese Schnittmenge von Marken
    """
    brand_combi_dir_name = "".join([brand['short'] for brand in brand_combi['brands']])
    return brand_combi_dir_name


for company in companies:
    for brand_combi in company['brand_combinations']:
        brand_combi['name'] = get_brand_combi_name(brand_combi=brand_combi)
    print('Example combination:')
    print(company['brand_combinations'][0]['name'])

# %% [markdown]
# ### Make queries list for BW

# %%
import datetime

def get_brand_combi_queries_dict(brand_combi, start_date="2010-07-01", languages=["en"], content_sources=["twitter", "blog", "forum", "reddit", "tumblr", "review", "news", "youtube"]):
    """Prepares a query formatted for the BWQueries upload() method from the specified brand_combination dict.

    Parameters:
        brand_combi (dict): Dictionary containing all necessary information about the brand combination
    
    Returns:
        dict: Dictionary formatted for the BWQueries upload() method
    """
    start = (datetime.date.fromisoformat(start_date) + datetime.timedelta(days=1)).isoformat() + "T05:00:00"
    query_dict = {
        'name': brand_combi['name'],
        'booleanQuery': brand_combi['keywords'],
        'startDate': start,
        'contentSources': content_sources,
        'languages': languages,
    }
    return query_dict


for company in companies:
    for brand_combi in company['brand_combinations']:
        brand_combi['query'] = get_brand_combi_queries_dict(brand_combi=brand_combi)
    print('Example combination query:')
    print(company['brand_combinations'][0]['query'])

# %% [markdown]
# ## Add group

# %%
from bcr_api.bwresources import BWGroups

groups = call_api(partial(BWGroups, project))

def get_bcr_group(company):
    """Uploads a company's brand combinations as queries in a group to BCR.

    Parameters:
        company (dict): Dictionary containing all necessary information about the company and its brands
    
    Returns:
        dict: Dictionary with the BCR group data
    """
    group_name = company['ticker']
    query_data_list = [brand_combi['query'] for brand_combi in company['brand_combinations']]
    upload_group_response_dict = call_api(partial(groups.upload_queries_as_group, group_name=group_name, query_data_list=query_data_list))
    upload_group_response_list = [(k, v) for k, v in upload_group_response_dict.items()]
    company_group = {
        'name': upload_group_response_list[0][0],
        'id': upload_group_response_list[0][1],
    }
    get_group_queries_res = call_api(partial(groups.get_group_queries, name=company_group['name']))
    get_group_queries_dict = dict(get_group_queries_res)
    get_group_queries_list = [(k, v) for k, v in get_group_queries_dict.items()]
    company_group_queries = [{'name': group_query_list[0], 'id': group_query_list[1]} for group_query_list in get_group_queries_list]
    company_group['queries'] = company_group_queries
    return company_group

for company in companies:
    company['group'] = get_bcr_group(company)
print('Example company group:')
print(companies[0]['group'])

# %% [markdown]
# ## Downloading data

# %%
from bcr_api.bwresources import BWQueries

queries = call_api(partial(BWQueries, project))

def get_bcr_query_data(company, query_id, aggregate, dimension=None, retries=10):
    """Retrieves data from BCR for a company's single brand combination (query) for two dimensions broken down by day.

    Parameters:
        company (dict): Dictionary containing all necessary information about the company and its brands
        query_id (str): String representing the BCR query ID for that brand combination
        aggregate (str): one of volume, authors, domains, impressions, netSentiment, reachEstimate, 
            twitterFollowers (see https://developers.brandwatch.com/docs/chart-dimensions-and-aggregates)
        dimension (str | None): one of None (used for queries and queryGroup) or sentiment, authors, languages, continents,
            countries, cities, regions, accountType, gender, interest, profession, pageTypes, domains,
            threadEntryTypes, emotions (see https://developers.brandwatch.com/docs/chart-dimensions-and-aggregates)
        retries (int): Number of retries for API call before error is thrown and None is returned
    
    Returns:
        dict: Dictionary with the BCR data
    """
    acceptable_aggregates = ['volume', 'authors', 'domains', 'impressions', 'netSentiment', 'reachEstimate', 'twitterFollowers']
    if not aggregate in acceptable_aggregates:
        raise Exception('aggregate not valid, see https://developers.brandwatch.com/docs/chart-dimensions-and-aggregates')
    
    acceptable_dimensions = [
        'sentiment',
        'authors',
        'languages',
        'continents',
        'countries',
        'cities',
        'regions',
        'accountType',
        'gender',
        'interest',
        'profession',
        'pageTypes',
        'domains',
        'threadEntryTypes',
        'emotions'
    ]
    if not (dimension in acceptable_dimensions or dimension is None):
        raise Exception('dimension not valid, see https://developers.brandwatch.com/docs/chart-dimensions-and-aggregates')

    breakdown_by = 'days'
    
    if dimension is None:
        try:
            query_bcr_res = call_api(partial(queries.get_chart, name=query_id, startDate=start_date, y_axis=aggregate, x_axis='queries', breakdown_by=breakdown_by, queryId=query_id))
            # print(queries_bcr_res)
            bcr_data = query_bcr_res['results'][0]
        except Exception as exc:
            print(f'Error Query If: query_bcr_res(aggregate={aggregate}, dimension={dimension}), retries={retries}')
            print(exc)
            if retries >= 0:
                return get_bcr_query_data(company=company, query_id=query_id, aggregate=aggregate, dimension=dimension, retries=(retries-1))
            return None
            
    else:
        try:
            query_bcr_res = call_api(partial(queries.get_chart, name=query_id, startDate=start_date, y_axis=aggregate, x_axis=dimension, breakdown_by=breakdown_by, queryId=query_id))
            bcr_data = query_bcr_res['results']
        except Exception as exc:
            print(f'Error Query Else: group_bcr_res(aggregate={aggregate}, dimension={dimension}), retries={retries}')
            print(exc)
            if retries > 0:
                return get_bcr_query_data(company=company, query_id=query_id, aggregate=aggregate, dimension=dimension, retries=(retries-1))
            return None

    return bcr_data

def get_bcr_data(company, aggregate, dimension=None, retries=10):
    """Retrieves data from BCR for a company's brand combinations for two dimensions broken down by day.

    Parameters:
        company (dict): Dictionary containing all necessary information about the company and its brands
        aggregate (str): one of volume, authors, domains, impressions, netSentiment, reachEstimate, 
            twitterFollowers (see https://developers.brandwatch.com/docs/chart-dimensions-and-aggregates)
        dimension (str | None): one of None (used for queries and queryGroup) or sentiment, authors, languages, continents,
            countries, cities, regions, accountType, gender, interest, profession, pageTypes, domains,
            threadEntryTypes, emotions (see https://developers.brandwatch.com/docs/chart-dimensions-and-aggregates)
        retries (int): Number of retries for API call before error is thrown and None is returned
    
    Returns:
        dict: Dictionary with the BCR data
    """
    acceptable_aggregates = ['volume', 'authors', 'domains', 'impressions', 'netSentiment', 'reachEstimate', 'twitterFollowers']
    if not aggregate in acceptable_aggregates:
        raise Exception('aggregate not valid, see https://developers.brandwatch.com/docs/chart-dimensions-and-aggregates')
    
    acceptable_dimensions = [
        'sentiment',
        'authors',
        'languages',
        'continents',
        'countries',
        'cities',
        'regions',
        'accountType',
        'gender',
        'interest',
        'profession',
        'pageTypes',
        'domains',
        'threadEntryTypes',
        'emotions'
    ]
    if not (dimension in acceptable_dimensions or dimension is None):
        raise Exception('dimension not valid, see https://developers.brandwatch.com/docs/chart-dimensions-and-aggregates')

    breakdown_by = 'days'
    
    bcr_data = {}
    if dimension is None:
        try:
            group_bcr_res = call_api(partial(groups.get_chart, name=company['group']['name'], startDate=start_date, y_axis=aggregate, x_axis='queryGroups', breakdown_by=breakdown_by, queryGroupId=company['group']['id']))
            # print(group_bcr_res)
            group_bcr_data = group_bcr_res['results'][0]
            queries_bcr_res = call_api(partial(queries.get_chart, name=company['group']['queries'][0]['id'], startDate=start_date, y_axis=aggregate, x_axis='queries', breakdown_by=breakdown_by, queryGroupId=company['group']['id']))
            # print(queries_bcr_res)
            queries_bcr_data = queries_bcr_res['results']
            for query in queries_bcr_data:
                bcr_data[query['name']] = query
                # print(bcr_data[query['name']])
        except Exception as exc:
            print(f'Error If: group_bcr_res(aggregate={aggregate}, dimension={dimension}), retries={retries}')
            print(exc)
            if retries >= 0:
                return get_bcr_data(company=company, aggregate=aggregate, dimension=dimension, retries=(retries-1))
            return None
    else:
        try:
            group_bcr_res = call_api(partial(groups.get_chart, name=company['group']['name'], startDate=start_date, y_axis=aggregate, x_axis=dimension, breakdown_by='days', queryGroupId=company['group']['id']))
            group_bcr_data = group_bcr_res['results']
            for query in company['group']['queries']:
                query_bcr_data = get_bcr_query_data(company=company, query_id=query['id'], aggregate=aggregate, dimension=dimension)
                bcr_data[query['name']] = query_bcr_data
        except Exception as exc:
            print(f'Error Else: group_bcr_res(aggregate={aggregate}, dimension={dimension}, retries={retries})')
            print(exc)
            if retries > 0:
                return get_bcr_data(company=company, aggregate=aggregate, dimension=dimension, retries=(retries-1))
            return None

    bcr_data['0_group'] = group_bcr_data
    return bcr_data

metrics = [
    ['volume', 'sentiment'],
    ['volume', None],
    ['volume', 'emotions'],
    ['authors', None],
    ['netSentiment', None],
    ['volume', 'pageTypes'],
    ['netSentiment', 'pageTypes'],
    ['authors', 'pageTypes'],
    ['volume', 'countries'],
    ['netSentiment', 'countries'],
    ['authors', 'countries'],
    ['volume', 'threadEntryTypes'],
    ['netSentiment', 'threadEntryTypes'],
    ['authors', 'threadEntryTypes'],
    ['volume', 'gender'],
    ['netSentiment', 'gender'],
    ['authors', 'gender'],
    ['volume', 'profession'],
    ['netSentiment', 'profession'],
    ['authors', 'profession'],
    ['volume', 'interest'],
    ['netSentiment', 'interest'],
    ['authors', 'interest']
]

for company in companies:
    company['metrics_data'] = {}
    for metric_combination in metrics:
        metric_combination_name = f'{metric_combination[0]}'
        if metric_combination[1] is not None:
            metric_combination_name = f'{metric_combination[0]}_{metric_combination[1]}'
        company['metrics_data'][metric_combination_name] = get_bcr_data(company=company, aggregate=metric_combination[0], dimension=metric_combination[1])

# %%
# Download 0 data as query instead of group
company_group_query_name = '0_query'
first_brand_keywords = companies[0]['brands'][0]['keywords']
second_brand_keywords = companies[0]['brands'][1]['keywords']
company_group_query_text = f'({first_brand_keywords}) OR ({second_brand_keywords})'
# print(company_group_query_text)

languages = ["en"]
content_sources = ["twitter", "blog", "forum", "reddit", "tumblr", "review", "news", "youtube"]
upload_query_response = call_api(partial(queries.upload, name=company_group_query_name, booleanQuery=company_group_query_text, startDate=start_date, contentSources=content_sources, languages=languages))
upload_query_response_dict = dict(upload_query_response)
upload_query_response_list = [(k, v) for k, v in upload_query_response_dict.items()]
query_id = upload_query_response_list[0][1]
for metric_combination in metrics:
        metric_combination_name = f'{metric_combination[0]}'
        if metric_combination[1] is not None:
            metric_combination_name = f'{metric_combination[0]}_{metric_combination[1]}'
        if metric_combination_name in companies[0]['metrics_data']:
            companies[0]['metrics_data'][metric_combination_name][company_group_query_name] = get_bcr_query_data(company=companies[0], query_id=query_id, aggregate=metric_combination[0], dimension=metric_combination[1])


# %% [markdown]
# ## Export to csv

# %%
import pandas as pd
import json

def get_bcr_data_df(metric_data, metric_name):
    """Retrieves bcr data stored in dictionary from company dictionary and returns pandas DataFrame with that data.

    Parameters:
        metric_data (dict): Dictionary containing all necessary information about the BCR metric for the company and its brands
        metric_name (str): Previously collected and stored metric data, e.g. volume, sentiment, netSentiment_pageTypes
    
    Returns:
        pandas.DataFrame: DataFrame containing the metric data
    """
    if metric_data is None:
        return None

    dfs = []
    for query_name, query_data in metric_data.items():
        if isinstance(query_data, list): # e.g. volume_sentiment
            for metric_dimension_data in query_data:
                data = metric_dimension_data['values']
                df = pd.DataFrame(data=data)
                metric_dimension_name = metric_dimension_data['name']
                value_column_name = f'{metric_dimension_name}_{query_name}'
                df = df.rename(columns={'id': 'date', 'value': value_column_name})
                df = df[['date', value_column_name]]
                df = df.set_index('date')
                # print(df.head())
                dfs.append(df)

        elif isinstance(query_data, dict): # e.g. volume
            data = query_data['values']
            df = pd.DataFrame(data=data)
            value_column_name = f'{metric_name}_{query_name}'
            df = df.rename(columns={'id': 'date', 'value': value_column_name})
            df = df[['date', value_column_name]]
            df = df.set_index('date')
            # print(df.head())
            dfs.append(df)
        
        else:
            raise Exception('data for query is not formatted correctly, should be either list or dict')
        
    df_out = pd.concat(dfs, axis=1)
    return df_out


for company in companies:
    # print(company['metrics_data'])
    company_ticker = company['ticker']
    with open(f'{company_ticker}.json', 'w') as json_outfile:
        json.dump(company, json_outfile)
    dfs = []

    for metric_combination_name, metric_combination_data in company['metrics_data'].items():
        df = get_bcr_data_df(metric_data=metric_combination_data, metric_name=metric_combination_name)
        if df is not None:
            dfs.append(df)

    df_out = pd.concat(dfs, axis=1)
    print(df_out.info())
    print(df_out.head())
    df_out.to_csv(f'{company_ticker}_data.csv')


# %% [markdown]
# ## Deleting group

# %%
# call_api(partial(groups.deep_delete, name=companies[0]['ticker']))
