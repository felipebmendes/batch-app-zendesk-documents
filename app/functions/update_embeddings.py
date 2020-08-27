import logging
import pandas as pd
import pickle
import requests
from pycarol import Carol, Storage, Query
from pycarol.data_models import DataModel
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


def update_embeddings(named_query_name, id_column_name, filter_column_name, app_name, url):

    login = Carol()
    login.app_name = app_name

    documents = Query(login).named(named_query=named_query_name).go().results

    df = pd.DataFrame(documents)

    # Load Sentence model
    model = SentenceTransformer('/app/model/')

    df['question'] = df['question'].str.lower()

    # Embed a list of sentences
    sentence_embeddings = model.encode(df.question.values, convert_to_tensor=True)

    id_list = df[id_column_name].values

    filter_list = df[filter_column_name].values

    index_to_question_id_mapping = dict(enumerate(id_list))
    #question_id_to_index_mapping = {j: i for i,j in index_to_question_id_mapping.items()} 

    index_to_filter_mapping = dict(enumerate(filter_list))
    filter_to_index_mapping = {}
    for filter in set(filter_list):
        filter_to_index_mapping[filter] = [i for i, x in enumerate(filter_list) if x == filter]

    ### Save objects in Storage
    stg = Storage(login)

    save_object_to_storage(stg, sentence_embeddings, 'sentence_embeddings')
    save_object_to_storage(stg, index_to_question_id_mapping, 'index_to_question_id_mapping')
    #save_object_to_storage(stg, question_id_to_index_mapping, 'question_id_to_index_mapping')
    save_object_to_storage(stg, index_to_filter_mapping, 'index_to_filter_mapping')
    save_object_to_storage(stg, filter_to_index_mapping, 'filter_to_index_mapping')

    response = requests.get(url)

    if response.ok:
        return 'done'
    else:
        raise ValueError('Error updating embeddings on Carol App.')


def save_object_to_storage(storage, obj, filename):
    with open(filename, "bw") as f:
        pickle.dump(obj, f)
    storage.save(filename, obj, format='pickle')