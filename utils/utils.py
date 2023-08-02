import pandas as pd

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