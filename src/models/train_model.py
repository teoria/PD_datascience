# -*- coding: utf-8 -*-
import click
import logging

from datetime import datetime
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd

from sklearn.ensemble import RandomForestClassifier

from sklearn.model_selection import train_test_split
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from collections import Counter
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from joblib import dump, load

# %% Kmeans

def optimal_number_of_clusters(wcss):
    x1, y1 = 2, wcss[0]
    x2, y2 = 20, wcss[len(wcss) - 1]

    distances = []
    for i in range(len(wcss)):
        x0 = i + 2
        y0 = wcss[i]
        numerator = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1)
        denominator = np.sqrt((y2 - y1) * 2 + (x2 - x1) * 2)
        distances.append(numerator / denominator)

    return distances.index(max(distances)) + 2


def calculate_wcss(data):
    wcss = []
    for n in range(2, 21):
        kmeans = KMeans(n_clusters=n)
        kmeans.fit(X=data)
        wcss.append(kmeans.inertia_)

    return wcss


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    df_abt = pd.read_csv(f"{output_filepath}/abt_segmentation.csv")
    df_abt.loc[df_abt.State=="0",'State'] = "NA"
    df_abt.loc[df_abt.City=="0",'City'] = "NA"
    df_abt.head()

    #%%

    categorical = ['CourseName']
    df_abt_categorical = df_abt[categorical]
    df_abt_categorical.index = df_abt.index
    ohe = pd.get_dummies(df_abt_categorical)
    df_train_full = pd.concat([df_abt,ohe],axis=1)

    df_train_full.drop(categorical+['UniversityName','State','City',"region"], axis=1, inplace=True)
    df_train_full.index = df_train_full["Id"]
    df_train_full.head()
    best_features = ["Id",
                      'fileview_count',
                     'session_count',
                     'session_rate',
                     'fileview_rate',
                     'usage_weekly_mean',
                     'usage_weekly_count',
                     'payment_total',
                     'payment_monthly',
                     'cancelation_count',
                     'mobile']
    df_train_full = df_train_full[best_features]
    ### Clusterização com Kmeans



    #%%

    df_classification = df_train_full.drop('Id',axis=1)
    sc = StandardScaler()
    df_classification_scaled = sc.fit_transform(df_classification)
    n = 2
    kmeans = KMeans(n_clusters=n, init='random',
                    n_init=10, max_iter=300,
                    tol=1e-04, random_state=0)
    kmeans.fit(df_classification_scaled)
    clusters = kmeans.fit_predict(df_classification_scaled).copy()

    clusters_list = list(clusters)

    x = Counter(clusters_list).keys()
    y = Counter(clusters_list).values()
    print('número de pessoas por grupo: {}'.format(y))

    columns_names=df_classification.columns
    df_segmented_students = pd.DataFrame(df_classification, columns = columns_names, index = df_classification.index)
    df_segmented_students['id_cluster'] = clusters
    df_segmented_students['StudentId'] = df_train_full.index


    target = df_segmented_students['id_cluster']
    X = df_segmented_students.drop(['id_cluster','StudentId'], axis=1)
    #%%

    X_train, X_valid, y_train, y_valid = train_test_split(X, target, test_size = 0.8, random_state = 42)
    rf = RandomForestClassifier(n_estimators = 200,
                               n_jobs = -1,
                                class_weight='balanced_subsample',
                               random_state = 42)
    rf.fit(X_train, y_train)
    print( f'score_train = {rf.score(X_train, y_train)}' )

    #%%

    print( f'score_validation = {rf.score(X_valid, y_valid)}'  )

    path = Path(__file__).resolve().parents[2]

    dump(rf, f'{path}/models/user_cluster.joblib')
    print( f'model saved in = {path}/models'  )

    y_pred = rf.predict(X_valid)
    print(y_valid.values)
    print(y_pred)
    cf_matrix = confusion_matrix(y_valid.values, y_pred.round())
    # plt.figure(figsize=(10,7))
    sns.set(font_scale=1.4)  # for label size
    #sns.heatmap(df_cm, annot=True, annot_kws={"size": 4})  # font size
    sns_plot = sns.heatmap(cf_matrix / np.sum(cf_matrix), annot=True,
                fmt='.2%', cmap='Blues')
    sns_plot.figure.savefig("output.png")

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
