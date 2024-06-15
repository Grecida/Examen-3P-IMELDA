import requests
import pandas as pd
import hashlib
import time
from sqlalchemy import create_engine
import json

def fetch_countries_data():
    """
    Función para obtener los datos de los países desde la API restcountries.com.
    Retorna la lista de países.
    """
    api_url = 'https://restcountries.com/v3.1/all'
    response = requests.get(api_url)
    countries = response.json()
    return countries

def process_countries_data(countries):
    """
    Función para procesar los datos de los países y generar el DataFrame.
    Retorna el DataFrame procesado.
    """
    data = []

    for country in countries:
        start_time = time.time()

        region = country.get('region', 'N/A')
        city_name = country.get('name', {}).get('common', 'N/A')
        languages = country.get('languages', {})
        if languages:
            language_name = list(languages.values())[0]
        else:
            language_name = 'N/A'

        # Encriptar el nombre del idioma con SHA1
        sha1 = hashlib.sha1(language_name.encode()).hexdigest()

        # Simular un pequeño retraso para medir el tiempo
        time.sleep(0.1)  # Añadir un retraso de 100 milisegundos

        # Calcular el tiempo tomado
        end_time = time.time()
        processing_time = (end_time - start_time) * 1000  # Convertir a milisegundos

        # Añadir los datos a la lista
        data.append({
            "Región": region,
            "City Name": city_name,
            "Language": sha1,
            "Time": f"{processing_time:.2f} ms"
        })

    # Crear el DataFrame
    df = pd.DataFrame(data)

    return df

def save_to_sqlite(df):
    """
    Función para guardar el DataFrame en una base de datos SQLite.
    """
    engine = create_engine('sqlite:///countries.db')
    df.to_sql('countries', engine, if_exists='replace', index=False)

def save_to_json(df):
    """
    Función para guardar el DataFrame como archivo JSON.
    """
    df.to_json('data.json', orient='records', lines=True)

def main():
    # Obtener datos de los países
    countries = fetch_countries_data()

    # Procesar datos y generar DataFrame
    df = process_countries_data(countries)

    # Mostrar estadísticas de tiempo
    total_time = df['Time'].apply(lambda x: float(x.split()[0])).sum()
    average_time = df['Time'].apply(lambda x: float(x.split()[0])).mean()
    min_time = df['Time'].apply(lambda x: float(x.split()[0])).min()
    max_time = df['Time'].apply(lambda x: float(x.split()[0])).max()

    print(f"Total Time: {total_time:.2f} ms")
    print(f"Average Time: {average_time:.2f} ms")
    print(f"Min Time: {min_time:.2f} ms")
    print(f"Max Time: {max_time:.2f} ms")

    # Guardar en SQLite
    save_to_sqlite(df)

    # Guardar como JSON
    save_to_json(df)

if __name__ == "__main__":
    main()
