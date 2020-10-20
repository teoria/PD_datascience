# -*- coding: utf-8 -*-
import click
import logging

from datetime import datetime
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
    logger.info('making features data set from process data')
    create_database_ABT(input_filepath, output_filepath)


def get_registered_time(df, max_time):
    tempos = []
    for student_reg_date in df.RegisteredDate:
        diff_time = pd.to_timedelta(pd.to_datetime(max_time) - pd.to_datetime(student_reg_date)).days
        tempos.append(diff_time)
    return tempos

def get_region(df):

    norte = ['Acre', 'Amapa', 'Amazonas', 'Pará', 'Rondonia', 'Roraima', 'Tocantins']
    nordeste = ['Alagoas', 'Bahia', 'Ceara', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte',
                'Sergipe']
    centro_oeste = ['Goias', 'Mato Grosso', 'Mato Grosso do Sul', 'Distrito Federal']
    sul = ['Rio Grande do Sul', 'Santa Catarina', 'Paraná']
    sudeste = ['Espirito Santo', 'Minas Gerais', 'Rio de Janeiro', 'São Paulo']
    na = ['NA']

    regions = []
    for student_state in df.State.fillna("NA"):
        region = next(
            n for n, v in filter(lambda t: isinstance(t[1], list), locals().items()) if student_state in v)
        regions.append(region)
    return regions

def create_database_ABT(input_filepath, output_filepath):
    '''

    ## Features
    - StudentId
    - registered_time(days)
    - usage_weekly_count(com
    session)
    - usage_weekly_mean(com
    session)
    - count_sessions
    - rate - sessions(qtd / tempobase)
    - count_file_view
    - Rate - fileview
    - count_question
    - rate - question
    - Região - ohe - OK
    - Estado - ohe - OK
    - Device_type
    - count_payment
    - count_payment_mensal
    - count_paymant_anual
    - count_cancellation
    - count_subject

    '''

    
    files = ['fileViews',
             'premium_cancellations',
             'premium_payments',
             'questions',
             'sessions',
             'students',
             'subjects',
             'cancellations_agg',
             'fileViews_agg',
             'payments_agg',
             'questions_agg',
             'sessions_agg',
             'subjects_agg',
             'usage_weekly',
             'usage_desktop_only',
             'usage_mobile_only'
             ]

    datasets = {}
    for file in files:
        datasets[file] = pd.read_csv(f"{output_filepath}/{file}.csv")  # ,parse_dates=[1,2],index_col=[0])
        print(f'*********{file}***********')
        print(datasets[file].shape)
        print(datasets[file].head(2))
        print("____________\n\n")
    
    fileViews = datasets.get('fileViews')
    cancellation = datasets.get('premium_cancellations')
    payment = datasets.get('premium_payments')
    questions = datasets['questions']
    sessions = datasets['sessions']
    student = datasets.get('students')
    subjects = datasets.get('subjects')
    cancellation_agg = datasets.get('cancellations_agg')
    fileViews_agg = datasets.get('fileViews_agg')
    payments_agg = datasets.get('payments_agg')
    questions_agg = datasets.get('questions_agg')
    sessions_agg = datasets.get('sessions_agg')
    subjects_agg = datasets.get('subjects_agg')
    usage_weekly = datasets.get('usage_weekly')
    usage_mobile = datasets.get('usage_mobile_only')
    usage_desktop = datasets.get('usage_desktop_only')

    
    max_time = sessions.SessionStartTime.max()
    sessions_agg = sessions_agg[['StudentId', 'SessionStartTime']]
    sessions_agg.columns = ['StudentId', 'session_count']
    
    fileViews_agg = fileViews_agg[['StudentId', 'FileName']]
    fileViews_agg.columns = ['StudentId', 'fileview_count']
    fileViews_agg.drop([45975, 45976], inplace=True)
    fileViews_agg.head()
    fileViews_agg['StudentId'] = fileViews_agg.StudentId.astype(int)
    
    questions_agg = questions_agg[['StudentId', 'QuestionDate']]
    questions_agg.columns = ['StudentId', 'question_count']

    
    cancellation_agg = cancellation_agg[['StudentId', 'CancellationDate']]
    cancellation_agg.columns = ['StudentId', 'cancelation_count']

    
    subjects_agg = subjects_agg[['StudentId', 'SubjectName']]
    subjects_agg.columns = ['StudentId', 'subject_count']


    
    student['registered_time'] = get_registered_time(student, max_time)
    student_usage = pd.merge(student,
                             usage_weekly,
                             left_on='Id',
                             right_on='StudentId',
                             how='left')
    

    
    student_usage_sessions = pd.merge(student_usage,
                                      sessions_agg,
                                      on='StudentId',
                                      suffixes='',
                                      how='left')

    student_usage_sessions['session_rate'] = student_usage_sessions['session_count'] / student_usage_sessions[
        'registered_time']
    
    student_usage_sessions_fileViews = pd.merge(student_usage_sessions,
                                                fileViews_agg[['StudentId', 'fileview_count']],
                                                on='StudentId',
                                                how='left')

    student_usage_sessions_fileViews['fileview_rate'] = student_usage_sessions_fileViews['fileview_count'] / \
                                                        student_usage_sessions_fileViews['registered_time']

    
    student_usage_sessions_fileViews_question = pd.merge(student_usage_sessions_fileViews,
                                                         questions_agg[['StudentId', 'question_count']],
                                                         on='StudentId',
                                                         how='left')

    student_usage_sessions_fileViews_question['question_rate'] = student_usage_sessions_fileViews_question[
                                                                     'question_count'] / \
                                                                 student_usage_sessions_fileViews_question[
                                                                     'registered_time']

    
    student_usage_sessions_fileViews_question['region'] = get_region(student_usage_sessions_fileViews_question)

    
    list_desktop = usage_desktop.StudentId.unique()
    list_mobile = usage_mobile.StudentId.unique()

    student_usage_sessions_fileViews_question['mobile'] = 0
    student_usage_sessions_fileViews_question['desktop'] = 0

    student_usage_sessions_fileViews_question.loc[
        student_usage_sessions_fileViews_question.StudentId.isin(list_mobile), 'mobile'] = 1
    student_usage_sessions_fileViews_question.loc[
        student_usage_sessions_fileViews_question.StudentId.isin(list_desktop), 'desktop'] = 1

    
    df_payment_total = payments_agg.groupby(['StudentId']).PaymentDate.sum().reset_index()
    df_payment_total_mensal = payments_agg.loc[payments_agg.PlanType == 'Mensal'].groupby(
        ['StudentId']).PaymentDate.sum().reset_index()
    df_payment_total_anual = payments_agg.loc[payments_agg.PlanType == 'Anual'].groupby(
        ['StudentId']).PaymentDate.sum().reset_index()
    df_payment_total.columns = ['StudentId', "payment_total"]
    df_payment_total_mensal.columns = ['StudentId', "payment_monthly"]
    df_payment_total_anual.columns = ['StudentId', "payment_yearly"]

    
    student_usage_sessions_fileViews_question_payment = pd.merge(student_usage_sessions_fileViews_question,
                                                                 df_payment_total,
                                                                 on='StudentId',
                                                                 how='left')
    student_usage_sessions_fileViews_question_payment_month = pd.merge(
        student_usage_sessions_fileViews_question_payment,
        df_payment_total_mensal,
        on='StudentId',
        how='left')
    student_usage_sessions_fileViews_question_payment_year = pd.merge(
        student_usage_sessions_fileViews_question_payment_month,
        df_payment_total_anual,
        on='StudentId',
        how='left')

    
    student_usage_sessions_fileViews_question_payment_year_cancellation = pd.merge(
        student_usage_sessions_fileViews_question_payment_year,
        cancellation_agg,
        on='StudentId',
        how='left')
    
    student_usage_sessions_fileViews_question_payment_year_cancellation_subject = pd.merge(
        student_usage_sessions_fileViews_question_payment_year_cancellation,
        subjects_agg,
        on='StudentId',
        how='left')

    
    df_abt = student_usage_sessions_fileViews_question_payment_year_cancellation_subject.fillna(0).copy()
    df_abt = df_abt[
         ['Id','UniversityName','CourseName','City','State','registered_time',
          'usage_weekly_count','usage_weekly_mean',
          'session_count','session_rate','fileview_count','fileview_rate',
          'question_count','question_rate','region','mobile','desktop',
          'payment_total','payment_monthly','payment_yearly',
          'cancelation_count','subject_count']
    ]
    df_abt.to_csv(f"{output_filepath}/abt_segmentation.csv", index=False)

    print("ABT criada")


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
