import os
import sys
import json
import glob
import csv
import time
import traceback
from datetime import datetime
import pytz

import numpy as np
from sentence_transformers import SentenceTransformer, util
from sklearn.cluster import AgglomerativeClustering


# Model for computing sentence embeddings. We use one trained for similar questions detection
# model = SentenceTransformer('all-MiniLM-L6-v2')
# model.max_seq_length = 12


# model = SentenceTransformer('all-MiniLM-L6-v2', device="cuda")
# sentences = ['This framework generates embeddings for each input sentence',
#     'Sentences are passed as a list of string.', 
#     'The quick brown fox jumps over the lazy dog.']
# sentence_embeddings = model.encode(sentences)

# for sentence, embedding in zip(sentences, sentence_embeddings):

#     print("Sentence:", len(sentence))
#     print(sentence[0])
#     print("Embedding:", len(embedding))
#     print("")

TIMEZONE = pytz.timezone('Asia/Singapore')

def init_sentence_transformer_model(pretrained_model_name='all-MiniLM-L6-v2', max_seq_length=256):
    model = SentenceTransformer(pretrained_model_name, device="cuda")
    model.max_seq_length = max_seq_length
    print("Max Sequence Length:", model.max_seq_length)
    return model

def make_log_fp(tpl_index):
    log_dir = "./clustering_log/"
    os.makedirs(log_dir, exist_ok=True)
    # tz = pytz.timezone('Asia/Singapore')
    log_file = os.path.join(log_dir, str(tpl_index)+".log")
    date_time = datetime.now(tz=TIMEZONE).strftime("%m/%d/%Y, %H:%M:%S")
    log_fp = open(log_file, "w+")
    print(date_time, file=log_fp)
    print(tpl_index, file=log_fp)
    return log_fp

def clustering(corpus_sentences, model, tpl_index):
    log_fp = make_log_fp(tpl_index)

    start = time.time()
    clustered_sentences = {}
    try:
        corpus_embeddings = model.encode(corpus_sentences, batch_size=64, show_progress_bar=True, convert_to_tensor=True)

        corpus_embeddings = corpus_embeddings.cpu().detach().numpy()
        corpus_embeddings = corpus_embeddings /  np.linalg.norm(corpus_embeddings, axis=1, keepdims=True)
        print(corpus_embeddings.shape)
        # with open("")
        # Perform kmean clustering without cluster number
        # following https://github.com/UKPLab/sentence-transformers/blob/master/examples/applications/clustering/agglomerative.py
        clustering_model = AgglomerativeClustering(n_clusters=None, distance_threshold=1.5) #, affinity='cosine', linkage='average', distance_threshold=0.4)
        clustering_model.fit(corpus_embeddings)
        cluster_assignment = clustering_model.labels_

        for sentence_id, cluster_id in enumerate(cluster_assignment):
            if cluster_id not in clustered_sentences:
                clustered_sentences[cluster_id.item()] = []

            clustered_sentences[cluster_id.item()].append(corpus_sentences[sentence_id])

        date_time = datetime.now(tz=TIMEZONE).strftime("%m/%d/%Y, %H:%M:%S")
        print(f"Finished at: {date_time}", file=log_fp)
        print("Running time: {:.2f}s".format(time.time()-start), file=log_fp)
    except Exception as e:
        # ex_type, ex, tb = sys.exc_info()
        # traceback.print_tb(tb, file=log_fp)
        traceback.print_exc(file=log_fp)
    log_fp.close()
    return clustered_sentences

def read_so_sents_for_tpl():
    with open("/app/additional_data/tpl_list.json", "r") as fp:
        tpls = json.load(fp)

    # for tpl in tpls:
    #     pass
    
    model = init_sentence_transformer_model()
    clustering_dir = "/app/search_result/clustering/"
    os.makedirs(clustering_dir, exist_ok=True)
    files_to_cluster = [f"/app/search_result/output/new_or/{i}.json" for i in list(range(0, 764))]
    files_to_cluster = set(files_to_cluster)
    file_count = 0
    for file_path in glob.glob("/app/search_result/output/new_or/*.json"):
        
        if file_path not in files_to_cluster:
            continue
        file_count += 1
        file_tpl_index = int(file_path.split(os.sep)[-1].split(".")[0])
        tpl_name = tpls[file_tpl_index]
        print(f"File: {file_count}: {tpl_name}- id: {file_tpl_index}")
        if os.path.exists(f"{clustering_dir}{file_tpl_index}.json"):
            continue
        with open(file_path, "r") as sents_fp:
            try:
                loaded_json = json.load(sents_fp)
            except Exception as e:
                log_dir = "./clustering_log/"
                os.makedirs(log_dir, exist_ok=True)
                log_fp = make_log_fp(file_tpl_index)
                traceback.print_exc(file=log_fp)
                log_fp.close()
                continue
            tpl_name = loaded_json['tpl_name']
            tpl_threads = loaded_json['threads']
        all_sents = []
        for sents in tpl_threads.values():
            all_sents += sents
        print(f"No. sents of TPL {file_tpl_index}: {len(all_sents)}")
        all_sents = all_sents[:50000]
        clusters = clustering(all_sents, model, file_tpl_index)
        if len(list(clusters.keys())) == 0:
            continue
        else:
            # print
            # print(type(list(clusters.keys())[0]))
            out_data = {"tpl_index": file_tpl_index, "tpl_name": tpls[file_tpl_index], "sents": clusters}
            with open(f"{clustering_dir}{file_tpl_index}.json", "w+") as clt_fp:
                json.dump(out_data, clt_fp, indent=2)
            
        


def main():
    read_so_sents_for_tpl()

if __name__ == "__main__":
    main()