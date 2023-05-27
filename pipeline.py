import pandas as pd
import numpy as np
import re
from cleanco import basename
from fuzzywuzzy.fuzz import token_set_ratio
from pathlib import Path
import argparse
import os
from typing import List
from config import *


def read_data(file_path: Path) -> pd.DataFrame:

    """
    Read Input CSV as a DataFrame

    Args:
        file_path (Path): Path to CSV file
    
    Returns:
        data (pandas.DataFrame): Data readead and loaded 

    """
    
    assert file_path.is_file(), "Input file does not exist"

    data = pd.read_csv(file_path)
    return data


def remove_anomaly_data(dataframe: pd.DataFrame) -> pd.DataFrame:

    """
    Remove missing values and invalid IBANs

    Args:
        dataframe (pandas.DataFrame): Dataframe to be cleaned
    
    Returns:
        dataframe (pandas.DataFrame): Dataframe with cleaned data
    """

    dataframe.dropna(subset=[NAME_COLUMN, IBAN_COLUMN], inplace=True)
    dataframe.drop(dataframe.loc[dataframe[IBAN_COLUMN]==INVALID].index, inplace=True)
    return dataframe


def normalize_text(text: str) -> str:

    """
    Normalize text.

    Args:
        text (str): Text to be normalized

    Returns:
        text (str): Normalized text
    """

    text = text.lower()
    text = re.sub(r'[^\w\s]','', text)
    text = text.strip()
    text = re.sub(' +',' ', text)
    return text


def normalize_name_company(dataframe: pd.DataFrame) -> pd.DataFrame:

    """
    Normalize name of the Companies.

    Args:
        dataframe (pandas.DataFrame): Dataframe to be normalized

    Returns:
        dataframe (pandas.DataFrame): Dataframe with normalized data

    """

    dataframe[NORM_NAME] = dataframe[NAME_COLUMN].apply(lambda x: normalize_text(x))
    return dataframe


def remove_duplicates(dataframe: pd.DataFrame) -> pd.DataFrame:

    """
    Remove duplicates by normalized name and iban

    Args:
        dataframe (pandas.DataFrame): Dataframe to remove duplicate

    Returns:
        dataframe (pandas.DataFrame): Dataframe with duplicates removed

    """
    
    dataframe.drop_duplicates(subset=[NORM_NAME, IBAN_COLUMN], inplace=True)
    return dataframe


def process_duplicate_removal(file_path: Path) -> pd.DataFrame:

    """
    Apply pipeline process for remove duplicates

    Args:
        file_path (Path): CSV file path with the data
    
    Returns:
        dataframe (pandas.DataFrame): Dataframe with duplicates removed

    """

    data = read_data(file_path)
    data = remove_anomaly_data(data)
    data = normalize_name_company(data)
    data = remove_duplicates(data)
    return data


def remove_business_terms(text: str) -> str:

    """
    Remove business terms, likes Corp, GmbH, among others.
    
    Args:
        text (str): Text to remove business terms
    
    Returns:
        text (str): Text with business terms removed

    """

    text = basename(text)
    text = text.strip()
    text = re.sub(' +',' ', text)
    return text


def remove_terms_name_company(dataframe: pd.DataFrame) -> pd.DataFrame:

    """
    Remove business terms from company name column

    Args:
        dataframe (pandas.DataFrame): Dataframe with the data

    Returns:
        dataframe (pandas.DataFrame): Dataframe without business terms.

    """

    dataframe[NOT_BUSINESS_TERMS_NAME] = dataframe[NORM_NAME].apply(remove_business_terms)
    return dataframe


def remove_last_digits_iban(iban: str) -> str:

    """
    Remove last two digits of the IBAN

    Args:
        iban (str): Complete IBAN

    Return:
        iban (str): IBAN without last two digits
    """

    iban = re.sub(' +','', iban)
    iban = iban[:-2]
    iban = iban.strip()
    return iban


def generate_name_iban_normalized(dataframe: pd.DataFrame) -> pd.DataFrame:

    """
    Generate columns with the IBAN without last 2 digits, join the normalized name and iban
    and generate column with name without business terms and iban processed.

    Args:
        dataframe (pandas.DataFrame): Dataframe with the data

    Returns:
        dataframe (pandas.DataFrame): Dataframe with the processed data
    """

    dataframe[NORM_IBAN] = dataframe[IBAN_COLUMN].apply(remove_last_digits_iban)
    dataframe[NORM_NAME_IBAN] = dataframe[NORM_NAME] + ' ' + dataframe[IBAN_COLUMN]
    dataframe[NOT_BUSINESS_TERMS_NAME_NORM_IBAN] = \
        dataframe[NOT_BUSINESS_TERMS_NAME] + ' ' + dataframe[NORM_IBAN]
    return dataframe


def get_uniques_name_plus_iban(dataframe: pd.DataFrame) -> np.array:

    """
    Get unique value from names without terms concataned with iban processed.

    Args:
        dataframe (pandas.DataFrame): Dataframe with the data

    Returns:
        uniques (np.array): Array with the unique values
    """

    name_iban_norm_uniques = dataframe[NOT_BUSINESS_TERMS_NAME_NORM_IBAN].unique()
    return name_iban_norm_uniques


def get_company_match(actual_company: str, unique_companies: List[str]) -> str:

    """
    Get the highest match score (levenshtein distance) between a normalized
    name plus iban and the unique names without terms concataned with iban
    processed.

    Args:
        actual_company (str): Name to be matched
        unique_companies (List[str]): List of unique names without terms concataned with iban

    Returns:
        match (str): Name without terms with the highest match score
            
    """

    match_values = []
    for unique_company in unique_companies:
        match_score = token_set_ratio(unique_company, actual_company)
        match_values.append(match_score)
    company = unique_companies[np.array(match_values).argmax()].split()[:-1]
    return ' '.join(company).capitalize()


def generate_company_match(dataframe: pd.DataFrame, unique_company_name_iban) -> pd.DataFrame:

    """
    Process company match for entire dataset

    Args:
        dataframe (pandas.DataFrame): Dataframe with the data
        unique_company_name_iban (List[str]): List of unique names without terms concataned with iban

    Returns:
        dataframe (pandas.DataFrame): Dataframe with the data
    """

    dataframe[COMPANY] = dataframe[NORM_NAME_IBAN].apply(lambda x: 
                                                         get_company_match(x,unique_company_name_iban))
    return dataframe


def generate_group_by_company(dataframe: pd.DataFrame) -> pd.DataFrame:

    """
    Group by each assigned Company name for each row

    Args:
        dataframe (pandas.DataFrame): Dataframe with the data
    Returns:
        dataframe (pandas.DataFrame): Grouped data by final company
    """

    data = dataframe.groupby(COMPANY).agg({NAME_COLUMN:lambda x: list(set(x)),
                                           IBAN_COLUMN:lambda x: list(set(x))})
    
    return data


def process_entity_resolution(dataframe: pd.DataFrame) -> pd.DataFrame:

    """
    Apply pipeline process for entity resolution
    
    Args:
        dataframe (pandas.DataFrame): Dataframe with the data without duplicates

    Returns:
        dataframe (pandas.DataFrame): Dataframe with entity resolution

    """

    dataframe = remove_terms_name_company(dataframe)
    dataframe = generate_name_iban_normalized(dataframe)
    uniques_name_plus_iban = get_uniques_name_plus_iban(dataframe)
    dataframe = generate_company_match(dataframe, uniques_name_plus_iban)
    result_dataframe = generate_group_by_company(dataframe)
    return result_dataframe



if __name__ == "__main__":

    parser = argparse.ArgumentParser('Substitution Process')
    parser.add_argument('-f', '--input_file', type=str, required=True)
    parser.add_argument('-o', '--output_folder', type=str, required=True)
    args = parser.parse_args()

    input_file = Path(args.input_file)
    output_folder = Path(args.output_folder)

    assert output_folder.exists(), "Output folder does not exist"

    data_duplicate_removal = process_duplicate_removal(file_path=input_file)
    duplicate_removal_path = os.path.join(output_folder, 'duplicate_removal_output.csv')
    data_duplicate_removal.to_csv(duplicate_removal_path,
                                                              index=False)
    
    data_entity_resolution = process_entity_resolution(dataframe=data_duplicate_removal)
    entity_resolution_path = os.path.join(output_folder, 'entity_resolution_output.csv')
    data_entity_resolution.to_csv(entity_resolution_path)



