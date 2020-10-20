# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd




@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    create_database_A(input_filepath, output_filepath)
    create_database_B(input_filepath, output_filepath)
    create_database_agg(input_filepath, output_filepath)


def create_database_A(input_filepath, output_filepath):
    df = pd.read_json(f'{input_filepath}/BASE A/premium_students.json')
    df.to_csv(f"{output_filepath}/premium_students.csv", index=False)

def create_database_B(input_filepath, output_filepath):
    files = ['fileViews', 'premium_cancellations', 'premium_payments', 'questions', 'sessions', 'students', 'subjects']
    for file in files:
        pd.read_json(f'{input_filepath}/BASE B/{file}.json'). \
            to_csv(f"{output_filepath}/{file}.csv", index=False)

def create_database_agg(input_filepath, output_filepath):
    files = ['fileViews',
             'premium_cancellations',
             'premium_payments',
             'questions',
             'sessions',
             'students',
             'subjects'
             ]

    datasets = {}
    for file in files:
        datasets[file] = pd.read_csv(f"{output_filepath}/{file}.csv")  # ,parse_dates=[1,2],index_col=[0])
    print('init agg datasets')
    count_fileview_by_studentId(datasets.get('fileViews')).to_csv(f"{output_filepath}/fileViews_agg.csv", index=False)
    print('fileViews')
    count_cancellation(datasets.get('premium_cancellations')).to_csv(f"{output_filepath}/cancellations_agg.csv", index=False)
    print('premium_cancellations')
    count_payment(datasets.get('premium_payments')).to_csv(f"{output_filepath}/payments_agg.csv", index=False)
    print('premium_payments')
    count_question_by_studentId(datasets.get('questions')).to_csv(f"{output_filepath}/questions_agg.csv", index=False)
    print('questions')
    count_session_by_studentId(datasets.get('sessions')).to_csv(f"{output_filepath}/sessions_agg.csv", index=False)
    print('sessions')
    count_subject(datasets.get('subjects')).to_csv(f"{output_filepath}/subjects_agg.csv", index=False)
    print('subjects')
    get_usage_weekly(datasets.get('students'), datasets.get('sessions')).to_csv(f"{output_filepath}/usage_weekly.csv", index=False)
    print('weekly')

    mobile_only, desktop_only = get_device_type(datasets.get('fileViews'))
    mobile_only.to_csv(f"{output_filepath}/usage_mobile_only.csv", index=False)
    desktop_only.to_csv(f"{output_filepath}/usage_desktop_only.csv", index=False)
    print('get_device_type agg datasets')
    print('end agg datasets')

def get_device_type(file_views):
    file_views['is_mobile'] = [0 if item == 'Website' else 1 for item in file_views.Studentclient.values]
    file_views['OS_mobile'] = [0 if item == 'Website' else 1 for item in file_views.Studentclient.values]
    df_desktop = file_views.loc[file_views.is_mobile == 0].reset_index()
    df_mobile_version = pd.DataFrame(
        file_views.loc[file_views.is_mobile == 1].Studentclient.str.split('|', expand=True).values,
        columns=['OS', 'version', 'sdk']
        )
    df_mobile_orig = file_views.loc[file_views.is_mobile == 1].reset_index()
    df_mobile = pd.concat([df_mobile_orig, df_mobile_version], axis=1)

    desk_mobile = pd.DataFrame(
        set(df_mobile.StudentId).intersection(set(df_desktop.StudentId))
    ).count().values[0] / len(file_views.StudentId.unique())

    return (df_mobile,df_desktop)


def get_usage_weekly(student, sessions):
    sessions['week_year'] = pd.to_datetime(sessions.SessionStartTime).map(lambda x: x.strftime("%Y-%V"))
    session_agg = sessions.groupby(['week_year', 'StudentId']).agg({'week_year': ['count']}).reset_index()
    student_ids = student.Id.unique()
    lista = [session_agg.loc[session_agg.StudentId == id].week_year.agg(['count','mean'])['count'].values for id in student_ids]
    df = pd.DataFrame(lista)
    df.columns = ['usage_weekly_count', 'usage_weekly_mean']
    df['StudentId'] = student_ids
    return df

def count_session_by_studentId(sessions):
    df = sessions.groupby(['StudentId']).count().reset_index()
    return df

def count_fileview_by_studentId(file_views):
    df = file_views.groupby(['StudentId']).count().reset_index()
    return df

def count_question_by_studentId(questions):
    df = questions.groupby(['StudentId']).count().reset_index()
    return df

def count_payment(payments):
    df = payments.groupby(['StudentId', "PlanType"]).count().reset_index()
    return df

def count_cancellation(cancelation):
    df = cancelation.groupby(['StudentId']).count().reset_index()
    return df

def count_subject(subjects):
    df = subjects.groupby(['StudentId']).count().reset_index()
    return df

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
