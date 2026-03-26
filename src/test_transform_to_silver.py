import pandas as pd
# import pytest
from transform_to_silver import clean_legislator_data


def test_clean_legislator_data_filters_democrats():
    """
    PRUEBA: ¿Realmente se queda solo con los Demócratas y limpia el estado?
    """
    # 1. CREAMOS DATOS FALSOS (MOCK DATA)
    raw_data = pd.DataFrame({
        'bioguideId': ['A1', 'B2', 'C3'],
        'name': ['Edgar', 'Pedro', 'Alfonso'],
        'partyName': ['Democratic', 'Republican', 'Democratic'],
        'state': ['CA', 'TX', 'ny']
    })

    processed_df = clean_legislator_data(raw_data)

    assert len(processed_df) == 2

    assert 'Republican' not in processed_df['partyName'].values

    assert processed_df.loc[
        processed_df['bioguideId'] == 'A1', 'state'
    ].values[0] == 'ca'

    assert processed_df.loc[processed_df.index[1], 'state'] == 'ny'


def test_filter_removes_republicans():
    raw_data = pd.DataFrame({
        'bioguideId': ['A1', 'B2', 'C3'],
        'name': ['Edgar', 'Pedro', 'Alfonso'],
        'partyName': ['Republican', 'Republican', 'Republican'],
        'state': ['CA', 'TX', 'ny']
    })

    processed_df = clean_legislator_data(raw_data)

    assert len(processed_df) == 0
