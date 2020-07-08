import re
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def normalize_articles(documents_df, sections_df, categories_df, body_column):
    
    sections_df = sections_df.astype(str)
    categories_df = categories_df.astype(str)
    documents_df = documents_df.astype(str)
    documents_df = documents_df.replace('', np.nan)
    documents_df = documents_df[(~documents_df[body_column].isnull())]
    logger.info(f'There are {documents_df.shape[0]} not empty documents.')

    # Merging Documents and Sections
    documents_df = documents_df.rename(columns={'name': 'article_name', 'id': 'article_id'})
    documents_df = pd.merge(documents_df, sections_df[['id', 'name', 'category_id']], left_on='section_id', right_on='id', how='left')
    documents_df = documents_df.drop(columns=['id'])
    documents_df = documents_df.rename(columns={'name': 'section_name'})

    # Merging Documents and Categories

    documents_df = pd.merge(documents_df, categories_df[['id', 'name']], left_on='category_id', right_on='id', how='left')
    documents_df = documents_df.drop(columns=['id'])
    documents_df = documents_df.rename(columns={'name': 'category_name', 'article_id': 'id'})
 
    # Normalizing Document's body
    documents_df['question'] = documents_df[body_column].apply(get_question)
    documents_df['question_type'] = documents_df[body_column].apply(get_question_type)
    documents_df['environment'] = documents_df[body_column].apply(get_environment)
    documents_df['solution'] = documents_df[body_column].apply(get_solution)
    documents_df['sanitized_solution'] = documents_df[body_column].apply(get_sanitized_solution)

    documents_df = documents_df.replace('', np.nan)
    documents_df = documents_df.dropna()
    documents_df = documents_df.reset_index(drop=True)

    for column in documents_df.columns:
        if column == id or '_id' in column:
            documents_df[column] = documents_df[column].apply(lambda x: x.replace('.0', ''))

    return documents_df


def remove_html_tags(text):
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    return " ".join(re.sub(r'\s([?.!"](?:\s|$))', r'\1', text).strip().split())


def get_question(body):    
    body = body.replace('\n', '').replace('<br>', '')

    m = re.search('(?<=<strong>D(ú|u)vida).*?(?=<strong>Ambiente)', body, re.IGNORECASE)
    if m:
        return remove_html_tags(m.group(0))
        
    m = re.search('(?<=<strong>Ocorr(ê|e)ncia).*?(?=<strong>Ambiente)', body, re.IGNORECASE)
    if m:
        return remove_html_tags(m.group(0))
    
    return np.nan


def get_question_type(body):   
    body = body.replace('\n', '').replace('<br>', '')

    m = re.search('(?<=<strong>D(ú|u)vida).*?(?=<strong>Ambiente)', body, re.IGNORECASE)
    if m:
        return 'question'

    m = re.search('(?<=<strong>Ocorr(ê|e)ncia).*?(?=<strong>Ambiente)', body, re.IGNORECASE)
    if m:
        return 'occurrence'
    
    return np.nan


def get_environment(body):   
    body = body.replace('\n', '').replace('<br>', '')

    m = re.search('(?=<strong>Ambiente).*?(?=<strong>Solu(ç|c)(ã|a)o)', body, re.IGNORECASE)
    if not m:
        return np.nan
    return remove_html_tags(m.group(0))


def get_solution(body):  
    body = body.replace('\n', '').replace('<br>', '')

    m = re.search('(?<=<strong>Solu(ç|c)(ã|a)o)(?s)(.*$)', body, re.IGNORECASE)
    if not m:
        return np.nan
    return re.sub('\.(?!\s)(?!$)(?!com)', '. ', m.group(0))


def get_sanitized_solution(body):    
    body = body.replace('\n', '').replace('<br>', '')

    m = re.search('(?<=<strong>Solu(ç|c)(ã|a)o)(?s)(.*$)', body, re.IGNORECASE)
    if not m:
        return np.nan
    return re.sub('\.(?!\s)(?!$)(?!com)', '. ', remove_html_tags(m.group(0)))