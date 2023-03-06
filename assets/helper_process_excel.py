"""
Helper module to load an excel sheets into DF
"""

import pandas as pd
import unicodedata
import logging

logging.basicConfig(filename='Helper.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

def process_excel_to_df_list(**kwargs):
    """transforma an excel sheet with many tables into """
    file = kwargs.get('file')
    colname = 'Region'
    list_df_new_column = []
    logging.info('Comienzo del procesamiento')
    list_df = process_file(file=file)
    if list_df:
        list_df_transposed = transpose_df(list_df)
        logging.info(f'Se proceso correctamente el df')
        return list_df_transposed
    return 1



def process_file(**kwargs):
    """Create list of DF based on excel file """
    file = kwargs.get('file')
    df_list = []
    try:
        m_df = pd.ExcelFile(file)
    except Exception as e:
        logging.error(f'Error al procesar el archivo: {e}')
        raise FileNotFoundError(f'No se pudo leer el archivo: {e}')

    nrows = m_df.book.sheet_by_name('Índices aperturas').nrows
    skrow = 5
    skfooter = nrows - 51
    count_tables = 0
    while nrows > 0:
        if skfooter > 0:
            df = m_df.parse(2, skiprows=skrow,skipfooter=skfooter,
                            parse_dates=True).dropna(axis=0, how='all').replace('///', None)
            df_list.append(df)

            #print(df.columns[0])
            if count_tables < 1:
                skrow = skrow + 51
                skfooter = skfooter - 50
                nrows = nrows - 50
                count_tables = count_tables + 1
            else:
                skrow = skrow + 49
                skfooter = skfooter - 49
                nrows = nrows - 49
                count_tables = count_tables + 1
        else:
            break
    print(df_list[0].columns.name)
    return df_list


def transpose_df(list_df):
    """Apartir de una lista de DF, transpone los datos, formatea las fechas y los numeros."""
    new_list = []
    for df2 in list_df:
        #print(df2.columns[0])
        df1 = df2.T
        df = df1[1:]

        df.index = pd.to_datetime(df.index)
        df = df.apply(pd.to_numeric)
        df.columns = df1.iloc[0]
        df = normalize_columns(df)
        df['Fecha'] = df.index
        df['Region'] = df2.columns[0]
        new_list.append(df)
        logging.info(f'Se proceso correctamente la transposicion del df')
    return new_list


def add_new_colum_to_df(list_df, colname):
    """Apartir de una lista de DF, Agrega una nueva columna a un df, retorna una lista df"""
    new_list = []
    for df in list_df:
        df[colname] = df.columns.name
        new_list.append(df)

    return new_list


def remove_accents(input_str):
    """Remove accents of a given word"""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii.decode("utf-8")

def normalize_columns(df):
    """Normalize columns name"""
    list_names = []
    list_col_names = df.columns
    for col_name in list_col_names:
        list_names.append(remove_accents(col_name[:58].title().replace(' ','').replace(',','').replace('.','')))

    df.columns = list_names
    logging.info(f'Se proceso correctamente la normalizacion de los nombres de las columnas del df')
    return df