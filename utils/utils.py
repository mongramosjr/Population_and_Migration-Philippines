import pandas as pd
import geopandas as gpd

def transform_csv_to_parquet(filename, folder):
    df = pd.read_csv(folder + filename + '.csv')
    df['geographic_level'] = pd.Series(["" for x in range(len(df.index))])
    df['geographic_level'].mask(df['Geographic Location']=="PHILIPPINES", 
                                       "Country", inplace=True)
    df['geographic_level'].mask(df['Geographic Location'].str.contains("^\.{4}"), 
                                       "Province", inplace=True)
    df['geographic_level'].mask((df['Geographic Location'].str.contains("City") 
                                       & ~df['Geographic Location'].str.contains("excluding")), 
                                       "City", inplace=True)
    df['geographic_level'].mask(df['Geographic Location'].str.contains("^\.{4}Pateros"), 
                                       "Municipality", inplace=True)
    
    df['geographic_level'].mask(df['Geographic Location'].str.contains("^\.{2}[a-z A-Z]+"), 
                                       "Region", inplace=True)
    df['geographic_level'].mask(df['Geographic Location'].str.contains("Region"), 
                                       "Region", inplace=True)
    
    df['Geographic Location'] = df['Geographic Location'].str.lstrip(".")
    df['Geographic Location'] = df['Geographic Location'].str.lstrip()
    df['Geographic Location'] = df['Geographic Location'].str.rstrip()

    filename = filename.lower()
    filename = filename.replace(" ", "_")
    df.to_parquet(folder + filename + '.parquet.gzip', compression='gzip') 
    
def fix_geographic_location_name(df):
    df["name"] = df["Geographic Location"]

    df['name'].replace(regex=True,inplace=True,to_replace=r'\(.*\)',value=r'')
    df['name'] = df['name'].str.lstrip()
    df['name'] = df['name'].str.rstrip()

    df['name'].replace(regex=True,inplace=True,
                                        to_replace=r'Autonomous Region In Muslim Mindanao',
                                        value=r'Bangsamoro Autonomous Region in Muslim Mindanao')

    # fixed name that contains "del"
    df['name'].replace(regex=True,inplace=True,
                                        to_replace=r'Del',
                                        value=r'del')

    # align name of Mimaropa Region
    df['name'].replace(regex=True,inplace=True,
                                        to_replace=r'Mimaropa Region',
                                        value=r'Region IV-B')
    return df

def merge_population_and_geodata(df_total_population):

    columns = ["name", "geometry"]
    
    gdf_location_region = gpd.read_parquet("../geojson_data/ph_administrative_boundaries/regions.001.parquet.gzip")
    df_population_region = pd.merge(df_total_population[df_total_population["geographic_level"]=="Region"],
                                    gdf_location_region[columns], left_on="name", right_on="name")
    
    gdf_location_province = gpd.read_parquet("../geojson_data/ph_administrative_boundaries/provinces.001.parquet.gzip")
    df_population_province = pd.merge(df_total_population[df_total_population["geographic_level"]=="Province"],
                                      gdf_location_province[columns], left_on="name", right_on="name")
    
    gdf_location_municipality = gpd.read_parquet("../geojson_data/ph_administrative_boundaries/municipalities.001.parquet.gzip")
    df_population_municipality = pd.merge(df_total_population[df_total_population["geographic_level"]=="Municipality"],
                                          gdf_location_municipality[columns], 
                                          left_on="name", right_on="name")
    df_population_city = pd.merge(df_total_population[df_total_population["geographic_level"]=="City"],
                                  gdf_location_municipality[columns],
                                  left_on="name", right_on="name")
    
    df_population_ = pd.concat([df_population_region, df_population_province,
                                df_population_city, df_population_municipality], ignore_index=True)
    population_gpd = gpd.GeoDataFrame(df_population_, crs="EPSG:4326")
    
    return population_gpd