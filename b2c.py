from datetime import datetime, timedelta
import pandas as pd
import tabula
import json

def transform():
    pd.set_option('display.max_columns', None)
    datasource_path = "/opt/airflow/dags/data_source/b2c"
    output_path = "/opt/airflow/dags/output/b2c"

    def ingestion_n_transform(year, source_file_name, page_scrap_list, col_drop, col_rename, series_dict):
        file_path = "{}/{}".format(datasource_path, source_file_name)

        tables = tabula.read_pdf(file_path, guess=False,
                                 pages=page_scrap_list, silent=True)
        df = pd.concat(tables)

        df.drop(col_drop, axis=1, inplace=True)
        df.rename(columns=col_rename, inplace=True)
        df['country'] = df['country'].str.replace('\r', ' ')

        # Create rank in piiar level
        df['Share of individuals with an account (15+) Rank'] = df['Share of individuals with an account (15+)'].rank(
            ascending=False, method='dense')
        df['Secure Internet servers (normalized) Rank'] = df['Secure Internet servers (normalized)'].rank(
            ascending=False, method='dense')
        df['Share of individuals using the Internet Rank'] = df['Share of individuals using the Internet'].rank(
            ascending=False, method='dense')
        df['UPU postal reliability Rank'] = df['UPU postal reliability score'].rank(
            ascending=False, method='dense')

        # Add col standard format
        i = 3
        df = pd.concat([df.iloc[:, :i],
                        pd.DataFrame('',
                                     columns=['sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar', 'indicator',
                                              'sub_indicator', 'others', 'unit_2'],
                                     index=df.index), df.iloc[:, i:]],
                       axis=1)

        df = df.melt(
            id_vars=['country', 'sub_index', 'pillar', 'sub_pillar',
                     'sub_sub_pillar', 'indicator', 'sub_indicator', 'others', 'unit_2'],
            var_name="Indicator",
            value_name="value"
        )

        def get_value_from_key(key, sub_key):
            return series_dict.get(key, {}).get(sub_key, "")

        df["pillar"] = df["Indicator"].apply(
            get_value_from_key, args=("pillar",))
        df["indicator"] = df["Indicator"].apply(
            get_value_from_key, args=("indicator",))
        df["unit_2"] = df["Indicator"].apply(
            get_value_from_key, args=("unit_2",))

        ingest_date = datetime.now()
        df['master_index'] = 'B2C'
        df['index'] = "UNCTAD B2C E-commerce Index "
        df['organizer'] = 'UNCTAD'
        df['ingest_date'] = ingest_date.strftime("%Y/%m/%d %H:%M")
        df['year'] = year
        df['unit_2'].replace(['rank', 'index', 'value'], [
            'Rank', 'Score', 'Score'], inplace=True)

        col = ["country", "year", "master_index", "organizer", "index", "sub_index", "pillar", "sub_pillar",
               "sub_sub_pillar",
               "indicator", "sub_indicator", "others", "unit_2", "value", "ingest_date"]

        df = df[col]
        df = df.sort_values(by=['country', 'year', 'pillar'])

        df.to_csv('{}/B2C_{}_{}.csv'.format(output_path, year,
                                            ingest_date.strftime("%Y%m%d%H%M%S")), index=False)

    def ingestion_init():
        # Get config
        json_file = open('{}/config.json'.format(datasource_path))
        conf_main = json.load(json_file)
        json_file.close()

        for config in conf_main:
            ingestion_n_transform(
                year=config['year'],
                source_file_name=config['source_file_name'],
                page_scrap_list=config['page_scrap_list'],
                col_drop=config['col_drop'],
                col_rename=config['col_rename'],
                series_dict=config['series_dict'],
            )

    ingestion_init()