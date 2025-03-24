# Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
from IPython.display import display
from io import StringIO
import csv
import gzip
from unidecode import unidecode

# URL of the Wikipedia page containing 25 tables with the contestants' information
url = "https://pt.wikipedia.org/wiki/Lista_de_participantes_do_Big_Brother_Brasil"

# Requesting the URL and locating the relevant tables
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
tables = soup.find_all("table", {"class": "wikitable"})

#Storing the tables into a dataframe with a table identifier 'Edicao' (Season).
dataframes = []

for i, table in enumerate(tables):  # Skip the first two tables
    df = pd.read_html(StringIO(str(table)))[0]  # Convert HTML table to DataFrame
    df['Edicao'] = f'{i+1}'  # Add a TableID column, start from Table_3
    dataframes.append(df)

contestants = pd.concat(dataframes, ignore_index=True)

# One of the tables had the Name column named differently, fixing this:
contestants['Nome completo'] = contestants['Nome completo'].fillna(contestants['Participantes']) 

# Removing spaces and accents from column names
contestants.rename(columns={'Nome completo': 'Nome'}, inplace=True)
contestants.rename(columns={'Data de nascimento': 'Data_Nascimento'}, inplace=True)
contestants.rename(columns={'Profissão': 'Profissao'}, inplace=True)

# Dropping irrelevant columns
contestants = contestants.drop(columns=['Ref.', 'Participantes'])

# Removing headers of subsequent tables
contestants = contestants[contestants["Origem"].str.contains("Origem") == False]

#There were nas under the Results column for the ongoing season. Replaced these with "Ongoing". 
contestants['Resultado'] = contestants['Resultado'].fillna(value="Em andamento em Em andamento")

# Some contestants are foreigners so we need an extra column to identify if they are Brazilian.
brazil_states = ['Acre','Alagoas','Amapá','Amazonas','Bahia','Ceará','Espírito Santo','Goiás','Maranhão','Mato Grosso','Mato Grosso do Sul','Minas Gerais', 'Pará','Paraíba','Paraná','Pernambuco','Piauí','Rio de Janeiro','Rio Grande do Norte','Rio Grande do Sul','Rondônia','Roraima','Santa Catarina','São Paulo','Sergipe','Tocantins','Distrito Federal']
contestants['Nacionalidade'] = contestants['Origem'].apply(lambda x: 'Brasileiro' if any(sub in x for sub in brazil_states) else 'Estrangeiro')

# Split the Origem column into City & State for Brazilians. For foreigners, it should say "Foreigner".
contestants[['Cidade','Estado']] = contestants['Origem'].str.split(', ', expand=True)
condition = contestants['Nacionalidade'] == 'Estrangeiro'
contestants.loc[condition,['Cidade']] = 'Estrangeiro'
contestants.loc[condition,['Estado']] = 'Estrangeiro'

#Removing the Origem column
contestants = contestants.drop(columns=['Origem'])

#Splitting the Resultado column to show the Result and Date separately
contestants[['Resultado','Data_Resultado']] = contestants['Resultado'].str.split(' em ', expand=True)

# Adding Gender using data from the Brazilian Census. When the names are not included in the census, it uses the gendered words in Resultado to determine Gender.

#Extracting the csv file with census data and storing it in a dictionary

def load_data():
    with gzip.open(r"C:\Users\andre\Desktop\Consultancy\Portfolio\Big Brother Brasil\nomes.csv.gz", mode="rt", encoding="utf-8") as fobj:
        csv_reader = csv.DictReader(fobj)
        data = {
        row["first_name"]: row["classification"]
            for row in csv_reader
        }
    return data

gender_dict = load_data()

# Function to extract first name from Name, uppercase it, and remove accents to match the dictionary formatting
def get_first_name(Nome):
    first_name = Nome.split()[0]
    first_name = unidecode(first_name)
    return first_name.upper()

# Apply the function to extract first names
contestants['Primeiro_Nome'] = contestants['Nome'].apply(get_first_name)
contestants['Genero'] = contestants['Primeiro_Nome'].map(gender_dict)

# For the Names not included in the census, try to identify the gender based on gendered words in Resultado.

# Function to determine gender based on the last letter of the Resultado column
def determine_gender(Resultado):
    if Resultado[-1].lower() == 'a':
        return 'F'
    elif Resultado[-1].lower() == 'o':
        return 'M'
    else:
        return 'NA'

# Apply the function only to rows where 'Gender' is NaN
contestants['Genero'] = contestants.apply(
    lambda row: determine_gender(row['Resultado']) if pd.isna(row['Genero']) else row['Genero'],
    axis=1
)

#Normalising gendered words in Resultado
contestants['Resultado'] = contestants['Resultado'].replace('Vencedora','Vencedor')
contestants['Resultado'] = contestants['Resultado'].str.replace(r'\beliminada\b', 'eliminado', regex=True)
contestants['Resultado'] = contestants['Resultado'].replace('Expulsa','Expulso')
contestants['Resultado'] = contestants['Resultado'].replace('Retirada','Retirado')

# Adding the year of each contestant show
contestants['Ano_Edicao'] = contestants['Data_Resultado'].str.slice(-4)
contestants.loc[contestants['Data_Resultado'] == 'Em andamento', "Ano_Edicao"] = "2025"

# Create unique ids for the contestants
unique_contestants = contestants['Nome'].unique()
name_to_id = {name: id for id, name in enumerate(unique_contestants, start=1)}
contestants['ID_Participante'] = contestants['Nome'].map(name_to_id)

#Normalising gendered words in Profissao
contestants['Profissao'] = contestants['Profissao'].str.replace('endedora', 'endedor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ssessora', 'ssessor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('dvogada', 'dvogado', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Atriz', 'Ator', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('atriz', 'ator', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('arwoman', 'arman', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('iomédica', 'iomédico', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ióloga', 'iólogo', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('abeleireira', 'abeleireiro', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('antora', 'antor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('irurgiã ', 'irurgião ', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('irurgiã-', 'irurgião-', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Aeromoça', 'Comissário de voo', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('onsultora', 'onsultor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ançarina', 'ançarino', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Dona', 'Dono', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('mpresária', 'mpresário', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('nfermeira', 'nfermeiro', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ngenheira', 'ngenheiro', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Criadora de conteúdo', 'Influenciador digital', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ogadora', 'ogador', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('utadora', 'utador', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('aquiadora', 'aquiador', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('presentadora', 'presentador', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('otogirl', 'otoboy', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('édica', 'édico', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('rodutora', 'rodutor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('rofessora', 'rofessor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('outora ', 'outor ', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('romotora', 'romotor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('écnica', 'écnico', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('sicóloga', 'sicólogo', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ublicitária', 'ublicitário', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('radutora', 'radutor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('peradora', 'perador', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('nfermeira', 'nfermeiro', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('youtuber', 'influenciador digital', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Youtuber', 'Influenciador digital', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('nfluenciadora digital', 'nfluenciador digital', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('posentada', 'posentado', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ducadora', 'ducador', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Ginasta', 'Atleta de ginástica artística', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ailarina', 'ailarino', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('onciliadora', 'onciliador', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('onfeiteira', 'onfeiteiro', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('oordenadora', 'oordenador', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('orretora', 'orretor', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Hostess', 'Host', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('ráfica', 'ráfico', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Doceira', 'Confeiteiro', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('musa', 'muso', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('grônoma', 'grônomo', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('uncionária pública', 'uncionário público', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('arota', 'aroto', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('otoqueiro', 'otoboy', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Paratleta', 'Atleta paratleta', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Surfista profissional', 'Surfista', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('Surfista', 'Atleta surfista', regex=True)
contestants['Profissao'] = contestants['Profissao'].str.replace('eterinária', 'eterinário', regex=True)

# Save the dataframe to CSV
contestants.to_csv(f'Contestants.csv', index=False)
