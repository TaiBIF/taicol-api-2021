# prepare data for get_conservation_info

import pandas as pd
import glob
import numpy as np


# CITES
df = pd.read_csv('/code/data/source-data/Index_of_CITES_Species_2023-03-13.csv',usecols=['TaxonId', 'Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus',
       'Species', 'Subspecies', 'FullName', 'AuthorYear', 'RankName','CurrentListing'])
df.to_csv('/code/data/conservation/cites.csv',index=None)


# 保育類名錄
df1 = pd.read_csv('/code/data/source-data/海洋保育類名錄.csv')
df2 = pd.read_csv('/code/data/source-data/陸域保育類名錄.csv')

df = df1.append(df2)
df = df.rename(columns={'Scientific Name(學名)': 'ScientificName', 'Common Name(俗名)': 'CommonName', 'Category(保育等級)': 'Category'})
df = df[['ScientificName','CommonName','Category']]

df[df.ScientificName.str.contains('\.', na=False)] # 全部都是spp.
df.ScientificName = df.ScientificName.apply(lambda x: x.replace('spp.', ''))
df.ScientificName = df.ScientificName.apply(lambda x: x.strip()) 

df.to_csv('/code/data/conservation/protected.csv',index=None)


# 敏感層級
files = glob.glob("/code/data/source-data/敏感層級_*")

df = pd.DataFrame()
for f in files:
    f_df = pd.read_csv(f)
    df = df.append(f_df)

df = df[['學名','敏感層級_預設','敏感層級_建議']]

df.to_csv('/code/data/conservation/sensitive.csv',index=None)


# IUCN
# 根據meta.xml的內容得知欄位名稱

df = pd.read_table('/code/data/source-data/iucn/Taxon.tsv', sep='\t')
df = df.replace({np.nan: ''})
df['dwc:scientificName'] = df.apply(lambda x: x['dwc:scientificName'].replace(x['dwc:scientificNameAuthorship'],''),axis=1)
df = df.rename(columns={'dwc:taxonID': 'iucn_id', 'dwc:scientificName': 'scientificName', 'dcterms:references':'url'})
df = df[df.url!='']
df = df[['iucn_id','scientificName','url']]

df.to_csv('/code/data/conservation/iucn.csv', index=None)

# df1 = pd.read_table('/code/data/source-data/iucn-2022-1/distribution.txt', header=None)
# df1 = df.rename(columns={0: 'iucn_id', 3: 'iucn_category'})

# df.merge(df1)

# 紅皮書
df = pd.read_csv('/code/data/source-data/臺灣紅皮書(2017).csv')
df = df[['category','criteria','adjusting','TaiCOL-accepted_name']]
df = df.rename(columns={'TaiCOL-accepted_name': 'source_name'})

df.to_csv('/code/data/conservation/redlist.csv', index=None)
